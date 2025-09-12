#ifndef HEADER_fd_src_app_geys_filter_h
#define HEADER_fd_src_app_geys_filter_h

#include "geys_fd_loop.h"
#include "../../flamenco/types/fd_types.h"

typedef struct GeyserSubscribeReactor GeyserSubscribeReactor_t;
typedef struct fd_blockstore fd_blockstore_t;
typedef struct geys_filter geys_filter_t;

geys_filter_t * geys_filter_create(fd_spad_t * spad, fd_funk_t * funk);

void geys_filter_set_service(geys_filter_t * filter, /* GeyserServiceImpl */ void * serv);

void geys_filter_add_sub(geys_filter_t * filter, /* SubscribeRequest*/ void * request, GeyserSubscribeReactor_t * reactor);

void geys_filter_un_sub(geys_filter_t * filter, GeyserSubscribeReactor_t * reactor);

void geys_filter_notify(geys_filter_t * filter, fd_replay_notif_msg_t * msg, int slot_complete, uchar * blk_data, ulong blk_sz);

#endif /* HEADER_fd_src_app_geys_filter_h */
