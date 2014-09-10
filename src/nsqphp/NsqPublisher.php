<?php
namespace nsqphp;

use nsqphp\Connection\Connection;
use nsqphp\Exception\PublishException;
use nsqphp\Message\MessageInterface;
use nsqphp\Wire\Reader;
use nsqphp\Wire\Writer;

class NsqPublisher
{
    /**
     * @var Connection
     */
    protected $connection;

    /**
     * @var Reader
     */
    protected $reader;

    /**
     * @var Writer
     */
    protected $writer;

    /**
     * @var int
     */
    protected $retries;

    /**
     * @var int
     */
    protected $retryDelay;

    /**
     * @var bool
     */
    protected $connected = false;

    /**
     * @param Connection $connection
     * @param int        $retries    Retries count on error [ -1 disabled | 0 infinite ]
     * @param int        $retryDelay Retries delay [msec]
     */
    public function __construct(Connection $connection, $retries = -1, $retryDelay = 100)
    {
        if (false == is_int($retries)) {
            throw new \InvalidArgumentException('$retries - has to be integer');
        }

        if (false == is_int($retryDelay) || $retryDelay < 0) {
            throw new \InvalidArgumentException('$retryDelay - has to be integer greater or equal zero');
        }

        $this->connection = $connection;
        $this->retries = $retries;
        $this->retryDelay = $retryDelay;

        $this->reader = new Reader();
        $this->writer = new Writer();
    }

    /**
     * Publish message
     *
     * @param string           $topic
     * @param MessageInterface $message
     *
     * @throws PublishException
     */
    public function publish($topic, MessageInterface $message)
    {
        $attempts = 0;
        while (true) {
            try {
                return $this->doPublish($topic, $message);
            } catch (\Exception $e) {
                $this->closeConnection();

                if (false == $this->retries) {
                    usleep($this->retryDelay);
                    continue;
                }

                if ($this->retries < 0 || ++$attempts >= $this->retries) {
                    throw $e;
                }

                usleep($this->retryDelay);
            }
        }
    }

    protected function closeConnection()
    {
        $this->connection->close();
        $this->connected = false;
    }

    protected function openConnection()
    {
        if (false == $this->connected) {
            $this->connection->write($this->writer->magic());
            $this->connected = true;
        }
    }

    /**
     * @param string           $topic
     * @param MessageInterface $msg
     */
    protected function doPublish($topic, MessageInterface $msg)
    {
        $this->openConnection();

        $this->connection->write($this->writer->publish($topic, $msg->getPayload()));

        while ($frame = $this->reader->readFrame($this->connection)) {
            if ($this->reader->frameIsHeartbeat($frame)) {
                continue;
            } else if ($this->reader->frameIsOk($frame)) {
                break;
            }

            throw new PublishException(sprintf('PUB failed "%s"', json_encode($frame)));
        }
    }
}
