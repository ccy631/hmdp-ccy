package com.hmdp.utils;

public interface ILock {

    boolean trylock(int timeoutSec);

    void unlock();
}
