package com.netflix.aegisthus.util;

import rx.Notification;
import rx.Observable;
import rx.Subscriber;
import rx.exceptions.Exceptions;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Most of this code is borrowed from rx.internal.operators.BlockingOperatorToIterator, which sadly does not offer a way
 * to apply backpressure.
 */

public class ObservableToIterator {
    public static <T> Iterator<T> toIterator(Observable<? extends T> source) {
        return toIterator(source, 25);
    }

    /**
     * Returns an iterator that iterates all values of the observable. Has bounded buffer, blocks source when buffer
     * gets full.
     */
    public static <T> Iterator<T> toIterator(Observable<? extends T> source, int bufferSize) {
        final BlockingQueue<Notification<? extends T>> notifications = new LinkedBlockingQueue<Notification<? extends T>>(bufferSize);

        // using subscribe instead of unsafeSubscribe since this is a BlockingObservable "final subscribe"
        source.materialize().subscribe(new Subscriber<Notification<? extends T>>() {
            @Override
            public void onCompleted() {
                // ignore
            }

            @Override
            public void onError(Throwable e) {
                // ignore
            }

            @Override
            public void onNext(Notification<? extends T> args) {
                try {
                    notifications.put(args);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        return new Iterator<T>() {
            private Notification<? extends T> buf;

            @Override
            public boolean hasNext() {
                if (buf == null) {
                    buf = take();
                }
                if (buf.isOnError()) {
                    throw Exceptions.propagate(buf.getThrowable());
                }
                return !buf.isOnCompleted();
            }

            @Override
            public T next() {
                if (hasNext()) {
                    T result = buf.getValue();
                    buf = null;
                    return result;
                }
                throw new NoSuchElementException();
            }

            private Notification<? extends T> take() {
                try {
                    return notifications.take();
                } catch (InterruptedException e) {
                    throw Exceptions.propagate(e);
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Read-only iterator");
            }
        };
    }
}
