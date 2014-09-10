<?php
namespace nsqphp\Event;

use Symfony\Component\EventDispatcher\Event as BaseEvent;

class Event extends BaseEvent
{
    /**
     * @var string
     */
    protected $topic;

    /**
     * @var string
     */
    protected $channel;

    /**
     * @param string $topic
     * @param string $channel
     */
    public function __construct($topic, $channel)
    {
        $this->topic = $topic;
        $this->channel = $channel;
    }

    /**
     * @return string
     */
    public function getTopic()
    {
        return $this->topic;
    }

    /**
     * @return string
     */
    public function getChannel()
    {
        return $this->channel;
    }
}
