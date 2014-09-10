<?php
namespace nsqphp\Event;

class Events
{
    const OK = 'ok';
    const HEARTBEAT = 'hearbeat';

    const MESSAGE = 'message';
    const MESSAGE_SUCCESS = 'message.success';
    const MESSAGE_ERROR = 'message.error';
    const MESSAGE_REQUEUE = 'message.requeue';
    const MESSAGE_DROP = 'message.drop';
    const MESSAGE_SKIP = 'message.skip';
}
