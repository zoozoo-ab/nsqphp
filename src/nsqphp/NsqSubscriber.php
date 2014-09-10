<?php
namespace nsqphp;

use nsqphp\Connection\Connection;
use nsqphp\Connection\ConnectionFactory;
use nsqphp\Event\Event;
use nsqphp\Event\Events;
use nsqphp\Event\MessageErrorEvent;
use nsqphp\Event\MessageEvent;
use nsqphp\Exception\LookupException;
use nsqphp\Lookup\LookupInterface;
use nsqphp\Message\Message;
use nsqphp\Wire\Reader;
use nsqphp\Wire\Writer;
use React\EventLoop\Factory;
use React\EventLoop\LoopInterface;
use Symfony\Component\EventDispatcher\EventDispatcherInterface;

class NsqSubscriber
{
    /**
     * @var LookupInterface
     */
    protected $lookupd;

    /**
     * @var ConnectionFactory
     */
    protected $connectionFactory;

    /**
     * @var Connection[]
     */
    protected $connections;

    /**
     * @var Reader
     */
    protected $reader;

    /**
     * @var Writer
     */
    protected $writer;

    /**
     * @var LoopInterface
     */
    protected $loop;

    /**
     * @var EventDispatcherInterface
     */
    protected $eventDispatcher;

    /**
     * @var bool
     */
    protected $running = false;

    /**
     * @var int
     */
    protected $processedMessageCount = 0;

    /**
     * @var int
     */
    protected $connectionLimit = 10;

    /**
     * @var int
     */
    protected $exitAfterMessages = 0;

    /**
     * @var int
     */
    protected $exitAfterTimeout = 0;

    /**
     * @param LookupInterface   $lookupd
     * @param ConnectionFactory $connectionFactory
     */
    public function __construct(LookupInterface $lookupd, ConnectionFactory $connectionFactory)
    {
        $this->lookupd = $lookupd;
        $this->connectionFactory = $connectionFactory;

        $this->loop = Factory::create();
        $this->reader = new Reader();
        $this->writer = new Writer();
        $this->connections = [];
    }

    /**
     * @return int
     */
    public function getConnectionLimit()
    {
        return $this->connectionLimit;
    }

    /**
     * lookupd can return many available NSQ hosts but you can restrict number outgoing connections.
     * subscribe shuffles received NSQ hosts so every new instance works with new set of NSQ hosts.
     *
     * @param int $connectionLimit
     */
    public function setConnectionLimit($connectionLimit)
    {
        if (is_int($connectionLimit) || $connectionLimit < 1) {
            throw new \InvalidArgumentException('Expected $connectionLimit is a integer more than one');
        }

        $this->connectionLimit = $connectionLimit;
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
     * @return int|null
     */
    public function getExitAfterMessages()
    {
        return $this->exitAfterMessages;
    }

    /**
     * @param int|null $exitAfterMessages
     */
    public function setExitAfterMessages($exitAfterMessages)
    {
        if (false == is_int($exitAfterMessages) || $exitAfterMessages < 0) {
            throw new \InvalidArgumentException('Expected $exitAfterMessages is a positive integer or zero');
        }

        $this->exitAfterMessages = $exitAfterMessages;
    }

    /**
     * @return int
     */
    public function getExitAfterTimeout()
    {
        return $this->exitAfterTimeout;
    }

    /**
     * @param int $seconds
     */
    public function setExitAfterTimeout($seconds)
    {
        if (false == is_int($seconds) || $seconds < 0) {
            throw new \InvalidArgumentException('Expected $exitAfterMessages is a positive integer or zero');
        }

        $this->exitAfterTimeout = $seconds;
    }

    /**
     * @param string   $topic
     * @param string   $channel
     * @param callable $callback
     */
    public function subscribe($topic, $channel, $callback)
    {
        if (false == is_callable($callback)) {
            throw new \InvalidArgumentException('Expected $callback is a PHP callable');
        }

        if (false == $hosts = $this->lookupd->lookupHosts($topic)) {
            throw new LookupException(sprintf('Could not find any available NSQ host for topic "%s"', $topic));
        }

        if ($this->connectionLimit && count($hosts) > $this->connectionLimit) {
            shuffle($hosts);
            $hosts = array_slice($hosts, 0, $this->connectionLimit);
        }

        foreach ($hosts as $host) {
            $parts = explode(':', $host);
            $connection = $this->connectionFactory->create($parts[0], $parts[1]);

            $socket = $connection->getSocket();
            $this->connections[(int) $socket] = $connection;
            $nsq = $this;
            $this->loop->addReadStream($socket, function ($socket) use ($nsq, $callback, $topic, $channel) {
                $nsq->readStream($socket, $topic, $channel, $callback);
            });

            // subscribe
            $connection->write($this->writer->magic());
            $connection->write($this->writer->subscribe($topic, $channel, '', ''));
            $connection->write($this->writer->ready(1));
        }

        $this->run();
    }

    protected function readStream($socket, $topic, $channel, $callback)
    {
        $connection = $this->connections[(int) $socket];
        $frame = $this->reader->readFrame($connection);

        if ($this->reader->frameIsHeartbeat($frame)) {
            $this->dispatchEvent(Events::HEARTBEAT, new Event($topic, $channel));
            $connection->write($this->writer->nop());
        } elseif ($this->reader->frameIsMessage($frame)) {
            $message = Message::fromFrame($frame);
            $this->dispatchMessage($connection, $message, $topic, $channel, $callback);
        } elseif ($this->reader->frameIsOk($frame)) {
            $this->dispatchEvent(Events::OK, new Event($topic, $channel));
        } else {
            // @todo handle error responses a bit more cleverly
            throw new Exception\ProtocolException("Error/unexpected frame received: " . json_encode($frame));
        }
    }

    protected function dispatchMessage($connection, $message, $topic, $channel, $callback)
    {
        try {
            $this->dispatchEvent(Events::MESSAGE, $event = new MessageEvent($message, $topic, $channel));

            if ($event->isProcessMessage()) {
                call_user_func($callback, $message);
                $connection->write($this->writer->finish($message->getId()));
                $this->dispatchEvent(Events::MESSAGE_SUCCESS, $event);
            } else {
                $connection->write($this->writer->finish($message->getId()));
                $this->dispatchEvent(Events::MESSAGE_SKIP, $event);
            }
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

        if ($this->exitAfterMessages > 0 && ++$this->processedMessageCount >= $this->exitAfterMessages) {
            $this->stop();
        }

        $this->running && $connection->write($this->writer->ready(1));
    }

    protected function run()
    {
        $this->running = true;

        if ($this->exitAfterTimeout > 0) {
            $that = $this;
            $this->loop->addTimer($this->exitAfterTimeout, function() use ($that) {
                $that->stop();
            });
        }

        $this->loop->run();
    }

    protected function stop()
    {
        $this->running = false;
        $this->loop->stop();
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
}
