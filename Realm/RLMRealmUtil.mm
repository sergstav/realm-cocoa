////////////////////////////////////////////////////////////////////////////
//
// Copyright 2014 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#import "RLMRealmUtil.h"

#import "RLMRealm_Private.hpp"

#import <sys/event.h>
#import <sys/stat.h>
#import <sys/time.h>
#import <unistd.h>

// A weak holder for an RLMRealm to allow calling performSelector:onThread:
// without a strong reference to the realm
@interface RLMWeakNotifier : NSObject {
@public
    int _fd;
}
@property (nonatomic, weak) RLMRealm *realm;
- (instancetype)initWithRealm:(RLMRealm *)realm;
- (void)stop;
@end

// Global realm state
static NSMutableDictionary *s_realmsPerPath = [NSMutableDictionary new];

void RLMCacheRealm(RLMRealm *realm) {
    @synchronized(s_realmsPerPath) {
        if (!s_realmsPerPath[realm.path]) {
            s_realmsPerPath[realm.path] = [NSMapTable mapTableWithKeyOptions:NSPointerFunctionsObjectPersonality
                                                                valueOptions:NSPointerFunctionsWeakMemory];
        }
        [s_realmsPerPath[realm.path] setObject:realm forKey:@(realm->_threadID)];
    }
}

RLMRealm *RLMGetAnyCachedRealmForPath(NSString *path) {
    @synchronized(s_realmsPerPath) {
        return [s_realmsPerPath[path] objectEnumerator].nextObject;
    }
}

RLMRealm *RLMGetCurrentThreadCachedRealmForPath(NSString *path) {
    mach_port_t threadID = pthread_mach_thread_np(pthread_self());
    @synchronized(s_realmsPerPath) {
        return [s_realmsPerPath[path] objectForKey:@(threadID)];
    }
}

void RLMClearRealmCache() {
    @synchronized(s_realmsPerPath) {
        [s_realmsPerPath removeAllObjects];
    }
}

void RLMStartListeningForChanges(RLMRealm *realm) {
    realm.notifier = [[RLMWeakNotifier alloc] initWithRealm:realm];
}

void RLMStopListeningForChanges(RLMRealm *realm) {
    [realm.notifier stop];
}

void RLMNotifyRealms(RLMRealm *notifyingRealm) {
    // Commits during schema init happen before the notifier is created, which
    // is okay because we explode if the file's schema is changed at a point
    // when there's someone listening for a change
    if (RLMWeakNotifier *notifier = notifyingRealm.notifier) {
        char c = 0;
        write(notifier->_fd, &c, 1);
    }
}

@implementation RLMWeakNotifier {
    CFRunLoopRef _runLoop;
    CFRunLoopSourceRef _signal;
    int _pipeFd[2];
    bool _cancel;
}

- (instancetype)initWithRealm:(RLMRealm *)realm {
    self = [super init];
    if (self) {
        _realm = realm;

        NSString *path = [realm.path stringByAppendingString:@".note"];
        mkfifo(path.UTF8String, 0777);
        _fd = open(path.UTF8String, O_RDWR);
        if (_fd <= 0) abort();

        errno = 0;
        int e = pipe(_pipeFd);
        assert(e == 0);

        _runLoop = CFRunLoopGetCurrent();

        CFRunLoopSourceContext ctx{};
        ctx.info = (__bridge void *)self;
        ctx.perform = [](void *info) {
            RLMWeakNotifier *notifier = (__bridge RLMWeakNotifier *)info;
            if (RLMRealm *realm = notifier->_realm) {
                [realm handleExternalCommit];
            }
        };
        _signal = CFRunLoopSourceCreate(0, 0, &ctx);
        CFRunLoopAddSource(_runLoop, _signal, kCFRunLoopDefaultMode);
        CFRelease(_signal);

        [self wait];
    }
    return self;
}

- (void)stop {
    // wake up the kqueue
    _cancel = true;
    char c = 0;
    int ret = write(_pipeFd[1], &c, 1);
    assert(ret == 1);
}

- (void)wait {
    dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{
        int _kq = kqueue();
        if (_kq <= 0) abort();

        struct kevent ke[2];
        EV_SET(&ke[0], _fd, EVFILT_READ, EV_ADD | EV_CLEAR, 0, 0, 0);
        EV_SET(&ke[1], _pipeFd[0], EVFILT_READ, EV_ADD | EV_CLEAR, 0, 0, 0);
        kevent(_kq, ke, 2, nullptr, 0, nullptr);

        while (true) {
            struct kevent ev;
            int ret = kevent(_kq, nullptr, 0, &ev, 1, nullptr);
            assert(ret > 0);
            if (ev.ident == _pipeFd[0]) {
                CFRunLoopSourceInvalidate(_signal);
                close(_fd);
                close(_pipeFd[0]);
                close(_pipeFd[1]);
                close(_kq);
                return;
            }
            CFRunLoopSourceSignal(_signal);
            CFRunLoopWakeUp(_runLoop);
        }
    });
}

@end
