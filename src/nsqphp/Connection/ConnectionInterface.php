<?php

namespace nsqphp\Connection;

interface ConnectionInterface
{
    public function close();

    /**
     * Read from the socket exactly $len bytes
     *
     * @param integer $len How many bytes to read
     * 
     * @return string Binary data
    */
    public function read($len);
    
    /**
     * Write to the socket.
     *
     * @param string $buf The data to write
     */
    public function write($buf);
    
    /**
     * Get socket handle
     * 
     * @return Resource The socket
     */
    public function getSocket();
}