#ifndef HEADER_fd_src_app_geys_fd_loop_h
#define HEADER_fd_src_app_geys_fd_loop_h

#include "../../util/fd_util.h"
#include "../../discof/replay/fd_replay_notif.h"
#include "../../funk/fd_funk.h"

struct geys_fd_loop_args {
  char funk_wksp[ 32 ];
  char store_wksp[ 32 ];
  char notify_wksp[ 32 ];
  char repair_wksp[ 32 ];
};

typedef struct geys_fd_loop_args geys_fd_loop_args_t;
typedef struct geys_fd_ctx geys_fd_ctx_t;
typedef struct geys_filter geys_filter_t;

geys_fd_ctx_t * geys_fd_init( geys_fd_loop_args_t * args );

void geys_fd_loop( geys_fd_ctx_t * ctx );

geys_filter_t * geys_get_filter( geys_fd_ctx_t * ctx );

#endif /* HEADER_fd_src_app_geys_fd_loop_h */
