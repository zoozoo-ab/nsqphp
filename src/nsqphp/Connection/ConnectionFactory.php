<?php
namespace nsqphp\Connection;

class ConnectionFactory
{
    /**
     * @var int
     */
    protected $connectionTimeout;

    /**
     * @var int
     */
    protected $readWriteTimeout;

    /**
     * @var int
     */
    protected $readWaitTimeout;

    /**
     * @var bool
     */
    protected $nonBlocking;

    /**
     * @param bool $nonBlocking
     * @param int  $connectionTimeout
     * @param int  $readWriteTimeout
     * @param int  $readWaitTimeout
     */
    public function __construct(
        $nonBlocking = false,
        $connectionTimeout = 3,
        $readWriteTimeout = 3,
        $readWaitTimeout = 15
    ) {
        if (false == is_int($connectionTimeout) || $connectionTimeout < 1) {
            throw new \InvalidArgumentException('Expected $connectionTimeout is a integer more than one');
        }

        if (false == is_int($readWriteTimeout) || $readWriteTimeout < 1) {
            throw new \InvalidArgumentException('Expected $readWriteTimeout is a integer more than one');
        }

        if (false == is_int($readWaitTimeout) || $readWaitTimeout < 1) {
            throw new \InvalidArgumentException('Expected $readWaitTimeout is a integer more than one');
        }

        $this->nonBlocking = (bool) $nonBlocking;
        $this->connectionTimeout = $connectionTimeout;
        $this->readWriteTimeout = $readWriteTimeout;
        $this->readWaitTimeout = $readWaitTimeout;
    }

    /**
     * @param string $host
     * @param string $port
     *
     * @return ConnectionInterface
     */
    public function create($host, $port)
    {
        return new Connection($host, $port, $this->nonBlocking, $this->connectionTimeout, $this->readWriteTimeout, $this->readWaitTimeout);
    }
}
