<?php
namespace nsqphp\Event;

use nsqphp\Message\MessageInterface;

class MessageErrorEvent extends AbstractMessageEvent
{
    /**
     * @var \Exception
     */
    protected $exception;

    /**
     * @var int
     */
    protected $requeueDelay;

    /**
     * @param MessageInterface $message
     * @param \Exception       $exception
     * @param string           $topic
     * @param string           $channel
     */
    public function __construct(MessageInterface $message, \Exception $exception, $topic, $channel)
    {
        parent::__construct($message, $topic, $channel);

        $this->exception = $exception;
    }

    /**
     * @return \Exception
     */
    public function getException()
    {
        return $this->exception;
    }

    /**
     * @return int
     */
    public function getRequeueDelay()
    {
        return $this->requeueDelay;
    }

    /**
     * @param int $requeueDelay
     */
    public function setRequeueDelay($requeueDelay)
    {
        $this->requeueDelay = $requeueDelay;
    }
}
