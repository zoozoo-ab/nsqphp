<?php

namespace nsqphp\Connection;

use nsqphp\Exception\ConnectionException;
use nsqphp\Exception\SocketException;

/**
 * Represents a single connection to a single NSQD server
 */
class Connection implements ConnectionInterface
{
    /**
     * Hostname
     * 
     * @var string
     */
    private $hostname;
    
    /**
     * Port number
     * 
     * @var integer
     */
    private $port;
    
    /**
     * Connection timeout - in seconds
     * 
     * @var float
     */
    private $connectionTimeout;
    
    /**
     * Read/write timeout - in whole seconds
     * 
     * @var integer
     */
    private $readWriteTimeoutSec;
    
    /**
     * Read/write timeout - in whole microseconds
     * 
     * (to be added to the whole seconds above)
     * 
     * @var integer
     */
    private $readWriteTimeoutUsec;

    /**
     * Read wait timeout - in whole seconds
     * 
     * @var integer
     */
    private $readWaitTimeoutSec;

    /**
     * Read wait timeout - in whole microseconds
     * 
     * (to be added to the whole seconds above)
     * 
     * @var integer
     */
    private $readWaitTimeoutUsec;

    /**
     * Non-blocking mode?
     *
     * @var boolean
     */
    private $nonBlocking;

    /**
     * Socket handle
     *
     * @var Resource|NULL
     */
    private $socket = NULL;

    /**
     * @var string
     */
    private $buffer = '';

    /**
     * Constructor
     *
     * @param string $hostname Default localhost
     * @param integer $port Default 4150
     * @param boolean $nonBlocking Put socket in non-blocking mode
     * @param float $connectionTimeout In seconds (no need to be whole numbers)
     * @param float $readWriteTimeout Socket timeout during active read/write
     *      In seconds (no need to be whole numbers)
     * @param float $readWaitTimeout How long we'll wait for data to become
     *      available before giving up (eg; duirng SUB loop)
     *      In seconds (no need to be whole numbers)
     */
    public function __construct(
        $hostname = 'localhost',
        $port = null,
        $nonBlocking = false,
        $connectionTimeout = 3,
        $readWriteTimeout = 3,
        $readWaitTimeout = 15
    ) {
        $this->hostname = $hostname;
        $this->port = $port ? $port : 4150;
        $this->nonBlocking = (bool) $nonBlocking;
        $this->connectionTimeout = $connectionTimeout;
        $this->readWriteTimeoutSec = floor($readWriteTimeout);
        $this->readWriteTimeoutUsec = ($readWriteTimeout - $this->readWriteTimeoutSec) * 1000000;
        $this->readWaitTimeoutSec = floor($readWaitTimeout);
        $this->readWaitTimeoutUsec = ($readWaitTimeout - $this->readWaitTimeoutSec) * 1000000;
    }

    /**
     * Close connection
     */
    public function close()
    {
        if (null === $this->socket) {
            return;
        }

        try {
            socket_shutdown($this->socket);
            socket_close($this->socket);
        } catch (\Exception $e) {}

        $this->socket = null;
    }
    
    /**
     * Read from the socket exactly $len bytes
     *
     * @param integer $len How many bytes to read
     * 
     * @return string Binary data
    */
    public function read($len)
    {
        $socket = $this->getSocket();
        $buffer = $data = '';
        while (strlen($data) < $len) {
            $buffer = stream_socket_recvfrom($socket, $len);

            if ($buffer === false) {
                throw new SocketException("Could not read {$len} bytes from {$this->hostname}:{$this->port}");
            } else if ($buffer == '') {
                throw new SocketException("Read 0 bytes from {$this->hostname}:{$this->port}");
            }

            $data .= $buffer;
            $len -= strlen($buffer);
        }
        return $data;
    }

    public function bufferData($data)
    {
        $this->buffer .= $data;
    }

    public function writeBuffer()
    {
        if ($this->buffer) {
            $this->write($this->buffer);
            $this->buffer = '';
        }
    }

    /**
     * Write to the socket.
     *
     * @param string $buf The data to write
     */
    public function write($buf)
    {
        $socket = $this->getSocket();

        // keep writing until all the data has been written
        while (strlen($buf) > 0) {
            // write buffer to stream
            $written = stream_socket_sendto($socket, $buf);

            if ($written === -1 || $written === false) {
                throw new SocketException("Could not write " . strlen($buf) . " bytes to {$this->hostname}:{$this->port}");
            }

            // determine how much of the buffer is left to write
            $buf = substr($buf, $written);
        }
    }
    
    /**
     * Get socket handle
     * 
     * @return Resource The socket
     */
    public function getSocket()
    {
        if (null === $this->socket) {
            $this->socket = fsockopen($this->hostname, $this->port, $errNo, $errStr, $this->connectionTimeout);

            if (false === $this->socket) {
                throw new ConnectionException("Could not connect to {$this->hostname}:{$this->port} ({$errStr} [{$errNo}])");
            }

            if ($this->nonBlocking) {
                stream_set_blocking($this->socket, 0);
            }
        }

        return $this->socket;
    }
    
    /**
     * To string (for debug logging)
     * 
     * @return string
     */
    public function __toString()
    {
        return "{$this->hostname}:{$this->port}";
    }
}