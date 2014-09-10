<?php
namespace nsqphp\Event;

use nsqphp\Message\MessageInterface;

class MessageEvent extends AbstractMessageEvent
{
    /**
     * @var bool
     */
    protected $processMessage;

    /**
     * @param MessageInterface $message
     * @param string           $topic
     * @param string           $channel
     */
    public function __construct(MessageInterface $message, $topic, $channel)
    {
        parent::__construct($message, $topic, $channel);

        $this->processMessage = true;
    }

    /**
     * @return boolean
     */
    public function isProcessMessage()
    {
        return $this->processMessage;
    }

    /**
     * @param boolean $processMessage
     */
    public function setProcessMessage($processMessage)
    {
        $this->processMessage = $processMessage;
    }
}
