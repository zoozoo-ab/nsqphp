<?php
namespace nsqphp\Event\Subscriber;

use nsqphp\Event\Event;
use nsqphp\Event\Events;
use nsqphp\Event\MessageErrorEvent;
use nsqphp\Event\MessageEvent;
use nsqphp\Logger\LoggerInterface;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;

class LoggerSubscriber implements EventSubscriberInterface
{
    /**
     * @var \nsqphp\Logger\LoggerInterface
     */
    protected $logger;

    /**
     * @param LoggerInterface $logger
     */
    public function __construct(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }

    /**
     * @param Event $event
     */
    public function onHeartbeat(Event $event)
    {
        $this->logger->debug(sprintf('HEARTBEAT topic:[%s] channel:[%s]', $event->getTopic(), $event->getChannel()));
    }

    /**
     * @param Event $event
     */
    public function onOk(Event $event)
    {
        $this->logger->debug(sprintf('OK topic:[%s] channel:[%s]', $event->getTopic(), $event->getChannel()));
    }

    /**
     * @param MessageEvent $event
     */
    public function onMessage(MessageEvent $event)
    {
        $this->logger->debug(sprintf('MESSAGE topic:[%s] channel:[%s] message:[%s]', $event->getTopic(), $event->getChannel(), $event->getMessage()->getId()));
    }

    /**
     * @param MessageEvent $event
     */
    public function onMessageSuccess(MessageEvent $event)
    {
        $this->logger->debug(sprintf('MESSAGE SUCCESS topic:[%s] channel:[%s] message:[%s] attempts:[%s]', $event->getTopic(), $event->getChannel(), $event->getMessage()->getId(), $event->getMessage()->getAttempts()));
    }

    /**
     * @param MessageErrorEvent $event
     */
    public function onMessageError(MessageErrorEvent $event)
    {
        $this->logger->debug(sprintf('MESSAGE ERROR topic:[%s] channel:[%s] message:[%s] attempts:[%s]', $event->getTopic(), $event->getChannel(), $event->getMessage()->getId(), $event->getMessage()->getAttempts()));
    }

    /**
     * @param MessageErrorEvent $event
     */
    public function onMessageRequeue(MessageErrorEvent $event)
    {
        $this->logger->debug(sprintf('MESSAGE REQUEUE topic:[%s] channel:[%s] message:[%s] attempts:[%s]', $event->getTopic(), $event->getChannel(), $event->getMessage()->getId(), $event->getMessage()->getAttempts()));
    }

    /**
     * @param MessageErrorEvent $event
     */
    public function onMessageDrop(MessageErrorEvent $event)
    {
        $this->logger->debug(sprintf('MESSAGE DROP topic:[%s] channel:[%s] message:[%s] attempts:[%s]', $event->getTopic(), $event->getChannel(), $event->getMessage()->getId(), $event->getMessage()->getAttempts()));
    }

    /**
     * {@inheritdoc}
     */
    public static function getSubscribedEvents()
    {
        return array(
            Events::OK => 'onOk',
            Events::HEARTBEAT => 'onHeartbeat',
            Events::MESSAGE => 'onMessage',
            Events::MESSAGE_SUCCESS => 'onMessageSuccess',
            Events::MESSAGE_REQUEUE => 'onMessageRequeue',
            Events::MESSAGE_DROP => 'onMessageDrop',
        );
    }
}
