<?php
namespace nsqphp\Event\Subscriber;

use nsqphp\Dedupe\DedupeInterface;
use nsqphp\Event\Events;
use nsqphp\Event\MessageEvent;
use nsqphp\Logger\LoggerInterface;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;

class DedupeSubscriber implements EventSubscriberInterface
{
    /**
     * @var DedupeInterface
     */
    protected $dedupe;

    /**
     * @var LoggerInterface
     */
    protected $logger;

    /**
     * @param DedupeInterface $dedupe
     */
    public function __construct(DedupeInterface $dedupe)
    {
        $this->dedupe = $dedupe;
    }

    /**
     * @param LoggerInterface $logger
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }

    /**
     * @param MessageEvent $event
     */
    public function onMessage(MessageEvent $event)
    {
        if (false == $this->dedupe->containsAndAdd($event->getTopic(), $event->getChannel(), $event->getMessage())) {
            return;
        }

        if ($this->logger) {
            $this->logger->debug(sprintf('Deduplicating topic:[%s] channel:[%s] message:[%s]', $event->getTopic(), $event->getChannel(), $event->getMessage()->getId()));
        }

        $event->setProcessMessage(false);
    }

    /**
     * @param MessageEvent $event
     */
    public function onMessageError(MessageEvent $event)
    {
        // erase knowledge of this msg from dedupe
        $this->dedupe->erase($event->getTopic(), $event->getChannel(), $event->getMessage());
    }

    /**
     * {@inheritdoc}
     */
    public static function getSubscribedEvents()
    {
        return array(
            Events::MESSAGE => 'onMessage',
            Events::MESSAGE_ERROR => 'onMessageError',
        );
    }
}
