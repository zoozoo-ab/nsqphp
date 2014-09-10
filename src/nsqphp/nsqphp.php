<?php

namespace nsqphp;

use nsqphp\Event\Event;
use nsqphp\Event\Events;
use nsqphp\Event\MessageErrorEvent;
use nsqphp\Event\MessageEvent;
use React\EventLoop\LoopInterface;
use React\EventLoop\Factory as ELFactory;

use nsqphp\Lookup\LookupInterface;
use nsqphp\Connection\ConnectionInterface;
use nsqphp\Message\MessageInterface;
use nsqphp\Message\Message;
use Symfony\Component\EventDispatcher\EventDispatcherInterface;

class nsqphp
{
    /**
     * Publish "consistency levels" [ish]
     */
    const PUB_ONE = 1;
    const PUB_TWO = 2;
    const PUB_QUORUM = 5;
    
    /**
     * nsqlookupd service
     * 
     * @var LookupInterface|NULL
     */
    private $nsLookup;

    /**
     * Connection timeout - in seconds
     * 
     * @var float
     */
    private $connectionTimeout;
    
    /**
     * Read/write timeout - in seconds
     * 
     * @var float
     */
    private $readWriteTimeout;
    
    /**
     * Read wait timeout - in seconds
     * 
     * @var float
     */
    private $readWaitTimeout;

    /**
     * Connection pool for subscriptions
     * 
     * @var Connection\ConnectionPool
     */
    private $subConnectionPool;

    /**
     * Connection pool for publishing
     * 
     * @var Connection\ConnectionPool|NULL
     */
    private $pubConnectionPool;
    
    /**
     * Publish success criteria (how many nodes need to respond)
     * 
     * @var integer
     */
    private $pubSuccessCount;

    /**
     * Event loop
     * 
     * @var LoopInterface
     */
    private $loop;
    
    /**
     * Wire reader
     * 
     * @var Wire\Reader
     */
    private $reader;
    
    /**
     * Wire writer
     * 
     * @var Wire\Writer
     */
    private $writer;
    
    /**
     * Long ID (of who we are)
     * 
     * @var string
     */
    private $longId;
    
    /**
     * Short ID (of who we are)
     * 
     * @var string
     */
    private $shortId;

    /**
     * @var bool
     */
    private $running = false;

    /**
     * @var EventDispatcherInterface
     */
    private $eventDispatcher;
    
    /**
     * Constructor
     * 
     * @param LookupInterface|NULL $nsLookup Lookup service for hosts from topic (optional)
     *      NB: $nsLookup service _is_ required for subscription
     */
    public function __construct(
        LookupInterface $nsLookup = NULL,
        $connectionTimeout = 3,
        $readWriteTimeout = 3,
        $readWaitTimeout = 15
    ) {
        $this->nsLookup = $nsLookup;
        
        $this->connectionTimeout = $connectionTimeout;
        $this->readWriteTimeout = $readWriteTimeout;
        $this->readWaitTimeout = $readWaitTimeout;
        $this->pubSuccessCount = 1;
        
        $this->subConnectionPool = new Connection\ConnectionPool;
        
        $this->loop = ELFactory::create();
        
        $this->reader = new Wire\Reader;
        $this->writer = new Wire\Writer;

        $hn = gethostname();
        $parts = explode('.', $hn);
        $this->shortId = $parts[0];
        $this->longId = $hn;
    }

    /**
     * @param EventDispatcherInterface $eventDispatcher
     */
    public function setEventDispatcher(EventDispatcherInterface $eventDispatcher)
    {
        $this->eventDispatcher = $eventDispatcher;
    }

    /**
     * @return EventDispatcherInterface
     */
    public function getEventDispatcher()
    {
        return $this->eventDispatcher;
    }

    /**
     * @param string $eventName
     * @param Event  $event
     */
    protected function dispatchEvent($eventName, Event $event)
    {
        if ($this->eventDispatcher) {
            $this->eventDispatcher->dispatch($eventName, $event);
        }
    }

    /**
     * Destructor
     */
    public function __destruct()
    {
        // say goodbye to each connection
        foreach ($this->subConnectionPool as $connection) {
            $connection->write($this->writer->close());
        }
    }
    
    /**
     * Define nsqd hosts to publish to
     * 
     * We'll remember these hosts for any subsequent publish() call, so you
     * only need to call this once to publish 
     * 
     * @param string|array $hosts
     * @param integer|NULL $cl Consistency level - basically how many `nsqd`
     *      nodes we need to respond to consider a publish successful
     *      The default value is nsqphp::PUB_ONE
     * 
     * @throws \InvalidArgumentException If bad CL provided
     * @throws \InvalidArgumentException If we cannot achieve the desired CL
     *      (eg: if you ask for PUB_TWO but only supply one node)
     * 
     * @return nsqphp This instance for call chaining
     */
    public function publishTo($hosts, $cl = NULL)
    {
        $this->pubConnectionPool = new Connection\ConnectionPool;

        if (!is_array($hosts)) {
            $hosts = explode(',', $hosts);
        }
        foreach ($hosts as $h) {
            if (strpos($h, ':') === FALSE) {
                $h .= ':4150';
            }
            
            $parts = explode(':', $h);
            $conn = new Connection\Connection(
                    $parts[0],
                    isset($parts[1]) ? $parts[1] : NULL,
                    $this->connectionTimeout,
                    $this->readWriteTimeout,
                    $this->readWaitTimeout,
                    FALSE      // blocking
                    );
            $this->pubConnectionPool->add($conn);
        }

        $this->publisher = new NsqPublisher($conn, 0);
        
        // work out success count
        if ($cl === NULL) {
            $cl = self::PUB_ONE;
        }
        switch ($cl) {
            case self::PUB_ONE:
            case self::PUB_TWO:
                $this->pubSuccessCount = $cl;
                break;
            case self::PUB_QUORUM:
                $this->pubSuccessCount = ceil($this->pubConnectionPool->count() / 2) + 1;
                break;
            default:
                throw new \InvalidArgumentException('Invalid consistency level');
                break;
        }
        if ($this->pubSuccessCount > $this->pubConnectionPool->count()) {
            throw new \InvalidArgumentException(sprintf('Cannot achieve desired consistency level with %s nodes', $this->pubConnectionPool->count()));
        }

        return $this;
    }
    
    /**
     * Publish message
     *
     * @param string $topic A valid topic name: [.a-zA-Z0-9_-] and 1 < length < 32
     * @param MessageInterface $msg
     * 
     * @throws Exception\PublishException If we don't get "OK" back from server
     *      (for the specified number of hosts - as directed by `publishTo`)
     * 
     * @return nsqphp This instance for call chaining
     */
    public function publish($topic, MessageInterface $msg)
    {
        $this->publisher->publish($topic, $msg);

        return $this;
    }    
    
    /**
     * Subscribe to topic/channel
     *
     * @param string $topic A valid topic name: [.a-zA-Z0-9_-] and 1 < length < 32
     * @param string $channel Our channel name: [.a-zA-Z0-9_-] and 1 < length < 32
     *      "In practice, a channel maps to a downstream service consuming a topic."
     * @param callable $callback A callback that will be executed with a single
     *      parameter of the message object dequeued. Simply return TRUE to 
     *      mark the message as finished or throw an exception to cause a
     *      backed-off requeue
     * 
     * @throws \RuntimeException If we don't have a valid callback
     * @throws \InvalidArgumentException If we don't have a valid callback
     * 
     * @return nsqphp This instance of call chaining
     */
    public function subscribe($topic, $channel, $callback)
    {
        if ($this->nsLookup === NULL) {
            throw new \RuntimeException(
                    'nsqphp initialised without providing lookup service (required for sub).'
                    );
        }
        if (!is_callable($callback)) {
            throw new \InvalidArgumentException(
                    '"callback" invalid; expecting a PHP callable'
                    );
        }
        
        // we need to instantiate a new connection for every nsqd that we need
        // to fetch messages from for this topic/channel

        $hosts = $this->nsLookup->lookupHosts($topic);

        foreach ($hosts as $host) {
            $parts = explode(':', $host);
            $conn = new Connection\Connection(
                    $parts[0],
                    isset($parts[1]) ? $parts[1] : NULL,
                    $this->connectionTimeout,
                    $this->readWriteTimeout,
                    $this->readWaitTimeout,
                    TRUE    // non-blocking
                    );

            $conn->write($this->writer->magic());
            $this->subConnectionPool->add($conn);
            $socket = $conn->getSocket();
            $nsq = $this;
            $this->loop->addReadStream($socket, function ($socket) use ($nsq, $callback, $topic, $channel) {
                $nsq->readAndDispatchMessage($socket, $topic, $channel, $callback);
            });
            
            // subscribe
            $conn->write($this->writer->subscribe($topic, $channel, $this->shortId, $this->longId));
            $conn->write($this->writer->ready(1));
        }
        
        return $this;
    }

    /**
     * Run subscribe event loop
     *
     * @param int $timeout (default=0) timeout in seconds
     */
    public function run($timeout = 0)
    {
        $this->running = true;

        if ($timeout > 0) {
            $that = $this;
            $this->loop->addTimer($timeout, function () use ($that) {
                $that->stop();
            });
        }
        $this->loop->run();
    }

    /**
     * Stop subscribe event loop
     */
    public function stop()
    {
        $this->running = false;
        $this->loop->stop();
    }
    
    /**
     * Read/dispatch callback for async sub loop
     * 
     * @param Resource $socket The socket that a message is available on
     * @param string $topic The topic subscribed to that yielded this message
     * @param string $channel The channel subscribed to that yielded this message
     * @param callable $callback The callback to execute to process this message
     */
    public function readAndDispatchMessage($socket, $topic, $channel, $callback)
    {
        $connection = $this->subConnectionPool->find($socket);
        $frame = $this->reader->readFrame($connection);

        // intercept errors/responses
        if ($this->reader->frameIsHeartbeat($frame)) {
            $this->dispatchEvent(Events::HEARTBEAT, new Event($topic, $channel));
            $connection->write($this->writer->nop());
        } elseif ($this->reader->frameIsMessage($frame)) {
            $message = Message::fromFrame($frame);
            
            try {
                $this->dispatchEvent(Events::MESSAGE, $event = new MessageEvent($message, $topic, $channel));

                $event->isProcessMessage() && call_user_func($callback, $message);
                $connection->write($this->writer->finish($message->getId()));

                $this->dispatchEvent(Events::MESSAGE_SUCCESS, $event);
            } catch (\Exception $e) {
                $this->dispatchEvent(Events::MESSAGE_ERROR, $event = new MessageErrorEvent($message, $e, $topic, $channel));

                if (null !== $event->getRequeueDelay()) {
                    $connection->write($this->writer->requeue($message->getId(), $event->getRequeueDelay()));
                    $this->dispatchEvent(Events::MESSAGE_REQUEUE, $event);
                } else {
                    $connection->write($this->writer->finish($message->getId()));
                    $this->dispatchEvent(Events::MESSAGE_DROP, $event);
                }
            }

            $this->running && $connection->write($this->writer->ready(1));

        } elseif ($this->reader->frameIsOk($frame)) {
            $this->dispatchEvent(Events::OK, new Event($topic, $channel));
        } else {
            // @todo handle error responses a bit more cleverly
            throw new Exception\ProtocolException("Error/unexpected frame received: " . json_encode($frame));
        }
    }
}
