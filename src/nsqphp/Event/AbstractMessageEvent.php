<?php
namespace nsqphp\Event;

use nsqphp\Message\MessageInterface;

abstract class AbstractMessageEvent extends Event
{
    /**
     * @var MessageInterface
     */
    protected $message;

    /**
     * @param MessageInterface $message
     * @param string           $topic
     * @param string           $channel
     */
    public function __construct(MessageInterface $message, $topic, $channel)
    {
        parent::__construct($topic, $channel);

        $this->message = $message;
    }

    /**
     * @return MessageInterface
     */
    public function getMessage()
    {
        return $this->message;
    }
}
