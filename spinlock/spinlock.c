#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <stdint.h>
#include <sched.h>
#include <assert.h>
#include <sys/time.h>
#include <errno.h>
//#include "spinlock.h"

typedef struct {
    volatile unsigned int ticket;
    volatile unsigned int turn;
} spinlock_t;

static inline void spinlock_init(spinlock_t * lock){

    // every thread takes a ticket
    lock->ticket = 0;
    // current turn for the matched ticket
    lock->turn = 0;
}

static inline void spinlock_lock(spinlock_t * lock, long id){
    unsigned int myturn = __sync_fetch_and_add(&lock->ticket, 1);
    while(lock->turn != myturn); //spin
    printf("dbg: thread id = %ld, lock->turn = %d, my turn = %d \n", id, lock->turn, myturn);

}

static inline int spinlock_unlock(spinlock_t * lock, long id){

    printf("dbg: thread id = %ld, prep to unlock!.\n", id);
    lock->turn += 1;

}

//----------------------------------------------------------------
















//asm volatile("": : :"memory");

#ifndef cpu_relax
#define cpu_relax() asm volatile("pause\n": : :"memory")
#endif

#define N_PAIR 1600
// Use an array of counter to see effect on RTM if touches more cache line.
#define NCOUNTER 1
#define CACHE_LINE 64
static __thread int8_t counter[CACHE_LINE*NCOUNTER];

spinlock_t sl;

static int nthr = 0;

volatile uint32_t wflag = 0;
/* Wait on a flag to make all threads start almost at the same time. */
void wait_flag(volatile uint32_t *flag, uint32_t expect) {
    __sync_fetch_and_add((uint32_t *)flag, 1);
    while (*flag != expect) {
        if(*flag%100==0)
            printf("flag = %d, expect = %d.", *flag, expect);
        cpu_relax();
    }
}

static struct timeval start_time;
static struct timeval end_time;

static void calc_time(struct timeval *start, struct timeval *end) {
    if (end->tv_usec < start->tv_usec) {
        end->tv_sec -= 1;
        end->tv_usec += 1000000;
    }

    assert(end->tv_sec >= start->tv_sec);
    assert(end->tv_usec >= start->tv_usec);
    struct timeval interval = {
        end->tv_sec - start->tv_sec,
        end->tv_usec - start->tv_usec
    };
    printf("%ld.%06ld\t", (long)interval.tv_sec, (long)interval.tv_usec);
}

void *inc_thread(void *id) {
    int n = N_PAIR / nthr;
    assert(n * nthr == N_PAIR);

    printf("thread %ld started.\n", (long)id);

	// wait for starting together
    wait_flag(&wflag, nthr);


    if (((long) id == 0)) {
        /*printf("get start time\n");*/
        gettimeofday(&start_time, NULL);
    }
    printf("thread %ld lock.\n", (long)id);

    /* Start lock unlock test. */
    for (int i = 0; i < n; i++) {
        spinlock_lock(&sl, (long)id);
        for (int j = 0; j < NCOUNTER; j++) counter[j*CACHE_LINE]++;
        printf("thread %ld in locking.\n", (long)id);
        spinlock_unlock(&sl, (long)id);
    }
    printf("thread %ld unlock.\n", (long)id);

    if (__sync_fetch_and_add((uint32_t *)&wflag, -1) == 1) {
        /*printf("get end time\n");*/
        gettimeofday(&end_time, NULL);
    }
    return NULL;
}

int main(int argc, const char *argv[])
{
    pthread_t *thr;
    int ret = 0;

    if (argc != 2) {
        printf("Usage: %s <num of threads>\n", argv[0]);
        exit(1);
    }

    spinlock_init(&sl);

    nthr = atoi(argv[1]);
    printf("using %d threads\n", nthr);
    thr = calloc(sizeof(*thr), nthr);

    // Start thread
    for (long i = 0; i < nthr; i++) {
        if (pthread_create(&thr[i], NULL, inc_thread, (void *)i) != 0) {
            printf("create thread %ld\n", i);
            perror("thread creating failed");
        }
    }
    // join thread
    for (long i = 0; i < nthr; i++)
        pthread_join(thr[i], NULL);

    calc_time(&start_time, &end_time);
    /*
     *for (int i = 0; i < NCOUNTER; i++) {
     *    if (counter[i] == N_PAIR) {
     *    } else {
     *        printf("counter %d error\n", i);
     *        ret = 1;
     *    }
     *}
     */

    return ret;
}
