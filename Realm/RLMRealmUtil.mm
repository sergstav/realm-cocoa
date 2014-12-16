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

// A weak holder for an RLMRealm to allow calling performSelector:onThread:
// without a strong reference to the realm
@interface RLMWeakNotifier : NSObject {
@public
    int _fd;
}
@property (nonatomic, weak) RLMRealm *realm;
- (instancetype)initWithRealm:(RLMRealm *)realm;
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
    realm.notifier = nil;
}

void RLMNotifyRealms(RLMRealm *notifyingRealm) {
    // Commits during schema init happen before the notifier is created, which
    // is okay because we explode if the file's schema is changed at a point
    // when there's someone listening for a change
    if (RLMWeakNotifier *notifier = notifyingRealm.notifier) {
        futimes(notifier->_fd, nullptr);
    }
}

@implementation RLMWeakNotifier {
@public
    struct kevent _ke;
    CFFileDescriptorRef _fdref;
}

static void RLMFileChanged(CFFileDescriptorRef fdref, CFOptionFlags, void *info) {
    RLMWeakNotifier *self = (__bridge RLMWeakNotifier *)info;
    struct kevent ev;
    timespec timeout = {0, 0};
    int ret = kevent(CFFileDescriptorGetNativeDescriptor(fdref), nullptr, 0, &ev, 1, &timeout);
    assert(ret > 0);
    if (RLMRealm *realm = self->_realm) {
        CFFileDescriptorEnableCallBacks(fdref, kCFFileDescriptorReadCallBack);
        [realm handleExternalCommit];
    }
}

- (instancetype)initWithRealm:(RLMRealm *)realm {
    self = [super init];
    if (self) {
        _realm = realm;

        _fd = open(realm.path.UTF8String, O_EVTONLY);
        if (_fd <= 0) abort();
        int kq = kqueue();
        if (kq <= 0) abort();

        EV_SET(&_ke, _fd, EVFILT_VNODE, EV_ADD | EV_CLEAR, NOTE_ATTRIB, 0, 0);
        kevent(kq, &_ke, 1, nullptr, 0, nullptr);

        CFFileDescriptorContext context = {0, (__bridge void *)self, nullptr, nullptr, nullptr};
        _fdref = CFFileDescriptorCreate(kCFAllocatorDefault, kq, true, RLMFileChanged, &context);

        CFFileDescriptorEnableCallBacks(_fdref, kCFFileDescriptorReadCallBack);
        CFRunLoopSourceRef source = CFFileDescriptorCreateRunLoopSource(kCFAllocatorDefault, _fdref, 0);
        CFRunLoopAddSource(CFRunLoopGetCurrent(), source, kCFRunLoopDefaultMode);
        CFRelease(source);
    }
    return self;
}

- (void)dealloc {
    CFFileDescriptorInvalidate(_fdref);
    CFRelease(_fdref);
    close(_fd);
}

@end
