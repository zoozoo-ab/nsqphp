<?php
namespace nsqphp\Event\Subscriber;

use nsqphp\Event\Events;
use nsqphp\Event\MessageErrorEvent;
use nsqphp\RequeueStrategy\RequeueStrategyInterface;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;

class RequeueSubscriber implements EventSubscriberInterface
{
    /**
     * @var RequeueStrategyInterface
     */
    protected $requeueStrategy;

    /**
     * @param RequeueStrategyInterface $requeueStrategy
     */
    public function __construct(RequeueStrategyInterface $requeueStrategy)
    {
        $this->requeueStrategy = $requeueStrategy;
    }

    /**
     * @param MessageErrorEvent $event
     */
    public function onMessageError(MessageErrorEvent $event)
    {
        $event->setRequeueDelay($this->requeueStrategy->shouldRequeue($event->getMessage()));
    }

    /**
     * {@inheritdoc}
     */
    public static function getSubscribedEvents()
    {
        return array(
            Events::MESSAGE_ERROR => 'onMessageError',
        );
    }
}
