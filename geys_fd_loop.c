#define _DEFAULT_SOURCE

#include "geys_fd_loop.h"
#include "geys_filter.h"
#include "../../tango/fd_tango_base.h"
#include "../../util/wksp/fd_wksp_private.h"
#include "../../disco/topo/fd_topo.h"
#include "../../discof/replay/fd_replay_notif.h"
#include "../../disco/store/fd_store.h"
#include "../../discof/reasm/fd_reasm.h"
#include <unistd.h>

#define SHAM_LINK_CONTEXT geys_fd_ctx_t
#define SHAM_LINK_STATE   fd_replay_notif_msg_t
#define SHAM_LINK_NAME    replay_sham_link
#include "sham_link.h"

#define SHAM_LINK_CONTEXT geys_fd_ctx_t
#define SHAM_LINK_STATE   fd_reasm_fec_t
#define SHAM_LINK_NAME    repair_sham_link
#include "sham_link.h"

#define GEYS_REASM_MAP_COL_CNT (1UL<<10)
#define GEYS_REASM_MAP_COL_HEIGHT (128UL)
struct geys_reasm_map {
  struct geys_reasm_map_column {
    ulong ele_cnt;  /* The number of shreds received in this column */    uchar end_found; /* Whether the last slice of the slot has been found */
    fd_reasm_fec_t ele[GEYS_REASM_MAP_COL_HEIGHT];
  } cols[GEYS_REASM_MAP_COL_CNT];
  ulong head; /* Next open column */
  ulong tail; /* Oldest column */
};
typedef struct geys_reasm_map geys_reasm_map_t;

struct geys_fd_ctx {
  fd_spad_t * spad;
  fd_funk_t funk_ljoin[1];
  fd_funk_t * funk;
  fd_store_t * store;
  geys_reasm_map_t * reasm_map;
  replay_sham_link_t * replay_notify;
  repair_sham_link_t * repair_notify;
  geys_filter_t * filter;
  uchar buffer[sizeof(fd_replay_notif_msg_t) > sizeof(fd_reasm_fec_t) ? sizeof(fd_replay_notif_msg_t) : sizeof(fd_reasm_fec_t)];
  int buffer_sz;
};
typedef struct geys_fd_ctx geys_fd_ctx_t;

geys_fd_ctx_t *
geys_fd_init( geys_fd_loop_args_t * args ) {
  geys_fd_ctx_t * ctx = (geys_fd_ctx_t *)malloc(sizeof(geys_fd_ctx_t));
  memset( ctx, 0, sizeof(geys_fd_ctx_t) );

  fd_wksp_t * funk_wksp = fd_wksp_attach( args->funk_wksp );
  if( FD_UNLIKELY( !funk_wksp ))
    FD_LOG_ERR(( "unable to attach to \"%s\"\n\tprobably does not exist or bad permissions", args->funk_wksp ));
  fd_wksp_tag_query_info_t info;
  ulong tag = 1;
  if( fd_wksp_tag_query( funk_wksp, &tag, 1, &info, 1 ) <= 0 ) {
    FD_LOG_ERR(( "workspace does not contain a funk" ));
  }
  void * funk_shmem = fd_wksp_laddr_fast( funk_wksp, info.gaddr_lo );
  for( int t = 0; t < 10; t++ ) {
    ctx->funk = fd_funk_join( ctx->funk_ljoin, funk_shmem );
    if( ctx->funk ) break;
    FD_LOG_WARNING(( "failed to join funk" ));
    sleep( 1 );
  }
  if( FD_UNLIKELY( !ctx->funk ))
    FD_LOG_ERR(( "failed to join funk" ));

  fd_wksp_t * store_wksp = fd_wksp_attach( args->store_wksp );
  if( FD_UNLIKELY( !store_wksp ))
    FD_LOG_ERR(( "unable to attach to \"%s\"\n\tprobably does not exist or bad permissions", args->store_wksp ));
  if( fd_wksp_tag_query( store_wksp, &tag, 1, &info, 1 ) <= 0 ) {
    FD_LOG_ERR(( "workspace does not contain a store" ));
  }
  void * store_shmem = fd_wksp_laddr_fast( store_wksp, info.gaddr_lo );
  fd_store_t * store = fd_store_join( store_shmem );
  if( FD_UNLIKELY( !store ))
    FD_LOG_ERR(( "failed to join store" ));
  ctx->store = store;

  ctx->reasm_map = (geys_reasm_map_t *)aligned_alloc( alignof(geys_reasm_map_t), sizeof(geys_reasm_map_t) );
  memset(ctx->reasm_map, 0, sizeof(geys_reasm_map_t));

#define SMAX 1LU<<30
  uchar * smem = aligned_alloc( FD_SPAD_ALIGN, SMAX );
  ctx->spad = fd_spad_join( fd_spad_new( smem, SMAX ) );
  fd_spad_push( ctx->spad );

  ctx->filter = geys_filter_create(ctx->spad, ctx->funk);

  ctx->replay_notify = replay_sham_link_new( aligned_alloc( replay_sham_link_align(), replay_sham_link_footprint() ), args->notify_wksp );

  ctx->repair_notify = repair_sham_link_new( aligned_alloc( repair_sham_link_align(), repair_sham_link_footprint() ), args->repair_wksp );

  return ctx;
}

geys_filter_t *
geys_get_filter( geys_fd_ctx_t * ctx ) {
  return ctx->filter;
}

void
geys_fd_loop( geys_fd_ctx_t * ctx ) {
  repair_sham_link_start( ctx->repair_notify );
  replay_sham_link_start( ctx->replay_notify );
  while( 1 ) {
    repair_sham_link_poll( ctx->repair_notify, ctx );
    replay_sham_link_poll( ctx->replay_notify, ctx );
  }
}

void
replay_sham_link_during_frag( geys_fd_ctx_t * ctx, ulong sig, ulong ctl, void const * msg, int sz ) {
  (void)sig;
  (void)ctl;
  FD_TEST( sz <= (int)sizeof(ctx->buffer) );
  memcpy(ctx->buffer, msg, (ulong)sz);
  ctx->buffer_sz = sz;
}

void
replay_sham_link_after_frag( geys_fd_ctx_t * ctx ) {
  if( ctx->buffer_sz != (int)sizeof(fd_replay_notif_msg_t) ) return;
  fd_replay_notif_msg_t * msg = (fd_replay_notif_msg_t *)ctx->buffer;
  if( msg->type != FD_REPLAY_SLOT_TYPE ) return;

  ulong slot = msg->slot_exec.slot;
  if( slot < ctx->reasm_map->tail || slot >= ctx->reasm_map->head ) return;
  ulong col_idx = slot & (GEYS_REASM_MAP_COL_CNT - 1);
  struct geys_reasm_map_column * col = &ctx->reasm_map->cols[col_idx];

  FD_LOG_NOTICE(( "processing slot %lu (%lu fec sets)", slot, col->ele_cnt ));

  fd_store_fec_t * list[GEYS_REASM_MAP_COL_HEIGHT];
  for( ulong idx = 0; idx < col->ele_cnt; ) {
    ulong end_idx = ULONG_MAX;
    FD_SPAD_FRAME_BEGIN( ctx->spad ) {

      /* Query the next batch */
      fd_store_shacq( ctx->store );
      ulong batch_sz = 0;
      for( ulong i = idx; i < col->ele_cnt; i++ ) {
        fd_reasm_fec_t * ele = &col->ele[i];
        fd_store_fec_t * fec_p = list[i-idx] = fd_store_query( ctx->store, &ele->key );
        if( !fec_p ) {
          fd_store_shrel( ctx->store );
          FD_LOG_WARNING(( "missing fec when assembling block %lu", slot ));
          return;
        }
        batch_sz += fec_p->data_sz;
        if( col->ele[i].data_complete ) {
          end_idx = i;
          break;
        }
      }
      if( end_idx == ULONG_MAX ) {
        fd_store_shrel( ctx->store );
        FD_LOG_WARNING(( "missing data complete flag when assembling block %lu", slot ));
        return;
      }
      uchar * blk_data = fd_spad_alloc( ctx->spad, alignof(ulong), batch_sz );
      ulong batch_off = 0;
      for( ulong i = idx; i <= end_idx; i++ ) {
        fd_store_fec_t * fec_p = list[i-idx];
        fd_memcpy( blk_data + batch_off, fec_p->data, fec_p->data_sz );
        batch_off += fec_p->data_sz;
      }
      FD_TEST( batch_off == batch_sz );
      fd_store_shrel( ctx->store );

      geys_filter_notify( ctx->filter, msg, col->ele[end_idx].slot_complete, blk_data, batch_sz );

    } FD_SPAD_FRAME_END;
    idx = end_idx + 1;
  }
}

void
repair_sham_link_during_frag( geys_fd_ctx_t * ctx, ulong sig, ulong ctl, void const * msg, int sz ) {
  (void)sig;
  (void)ctl;
  FD_TEST( sz <= (int)sizeof(ctx->buffer) );
  memcpy(ctx->buffer, msg, (ulong)sz);
  ctx->buffer_sz = sz;
}

void
repair_sham_link_after_frag( geys_fd_ctx_t * ctx ) {
  if( ctx->buffer_sz != (int)sizeof(fd_reasm_fec_t) ) return;
  fd_reasm_fec_t * fec_msg = (fd_reasm_fec_t *)ctx->buffer;
  geys_reasm_map_t * reasm_map = ctx->reasm_map;

  if( reasm_map->head == 0UL ) {
    reasm_map->head = fec_msg->slot+1;
    reasm_map->tail = fec_msg->slot;
  }
  if( fec_msg->slot < reasm_map->tail ) return; /* Do not go backwards */
  while( fec_msg->slot >= reasm_map->tail + GEYS_REASM_MAP_COL_CNT ) {
    FD_TEST( reasm_map->tail < reasm_map->head );
    reasm_map->tail++;
  }
  while( fec_msg->slot >= reasm_map->head ) {
    ulong col_idx = (reasm_map->head++) & (GEYS_REASM_MAP_COL_CNT - 1);
    struct geys_reasm_map_column * col = &reasm_map->cols[col_idx];
    col->ele_cnt = 0;
  }
  FD_TEST( fec_msg->slot >= reasm_map->tail && fec_msg->slot < reasm_map->head && reasm_map->head - reasm_map->tail <= GEYS_REASM_MAP_COL_CNT );

  ulong col_idx = fec_msg->slot & (GEYS_REASM_MAP_COL_CNT - 1);
  struct geys_reasm_map_column * col = &reasm_map->cols[col_idx];
  if( col->ele_cnt ) {
    FD_TEST( fec_msg->fec_set_idx > col->ele[col->ele_cnt-1].fec_set_idx );
  }
  FD_TEST( col->ele_cnt < GEYS_REASM_MAP_COL_HEIGHT );
  col->ele[col->ele_cnt++] = *fec_msg;
}
