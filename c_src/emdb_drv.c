/* -------------------------------------------------------------------------
 * This file is part of EMDB - Erlang MDB API                               
 *                                                                          
 * Copyright (c) 2012 by Aleph Archives. All rights reserved.               
 *                                                                          
 * -------------------------------------------------------------------------
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 * -------------------------------------------------------------------------*/

/*
 * C headers
 */

#include <sys/param.h>  /* for MAXPATHLEN constant */
#include <erl_nif.h>    /* for Erlang NIF interface */
#include <erl_driver.h> /* for driver_get_now*/
#include <lmdb.h>        /* for MDB interface */
#include "queue.h"


#define FREE(p)   (NULL == (p) ? 0 : (free(p), p = NULL))

#define FAIL_FAST(Error, Goto)                    \
  do{                                             \
    err = Error;                                  \
    goto Goto;                                    \
}while(0)

static ErlNifResourceType *emdb_map_type = NULL;

typedef struct emdb_map_t {
  MDB_env * env;
  MDB_dbi   dbi;
  MDB_cursor * cursor;
  MDB_txn * txn;  
  
  ErlNifTid tid;
  ErlNifThreadOpts* opts;
  queue *command_queue;
} emdb_map_t;

typedef enum {
    cmd_open,
    cmd_close,
    cmd_put,
    cmd_get,
    cmd_update,
    cmd_del,
    cmd_append,
    cmd_txn_begin,
    cmd_txn_begin_ro,
    cmd_txn_commit,
    cmd_txn_abort,
    cmd_drop,
    cmd_stop,
    cmd_cursor_open,
    cmd_cursor_close,
    cmd_cursor_next,
    cmd_cursor_set,
    cmd_cursor_del,
    cmd_unknown,
    cmd_dummy,
    cmd_bulk_txn
} command_type;


typedef struct {
  command_type type;
  
  ErlNifEnv *env;
  ERL_NIF_TERM ref; 
  ErlNifPid pid;
  ERL_NIF_TERM *args;
} emdb_command;


static struct emdb_map_t * emdb_map = NULL;


/* emdb ret */
#define EMDB_RET_KEY_EXIST           "key_exist"

/* emdb errors */
#define EMDB_MALLOC_ERR              "error_malloc"
#define EMDB_MAKE_BINARY_ERR         "error_make_binary"
#define EMDB_CREATE_ERR              "error_create"
#define EMDB_MAPSIZE_ERR             "error_mapsize"
#define EMDB_OPEN_ERR                "error_open"
#define EMDB_TXN_BEGIN_ERR           "error_txn_begin"
#define EMDB_TXN_COMMIT_ERR          "error_txn_commit"
#define EMDB_TXN_STARTED_ERR         "error_txn_already_started"
#define EMDB_TXN_FULL_ERR            "error_txn_full"
#define EMDB_TXN_ABORT_ERR           "error_txn_abort"
#define EMDB_NO_TXN_ERR              "error_not_in_a_transaction"
#define EMDB_OPEN_DBI_ERR            "error_open_dbi"
#define EMDB_INVALID_HANDLE_ERR      "error_invalid_handle"
#define EMDB_PUT_ERR                 "error_put"
#define EMDB_UPDATE_ERR              "error_update"
#define EMDB_KEY_NOT_FOUND           "error_key_not_found"
#define EMDB_DROP_ERR                "error_drop"
#define EMDB_CURSOR_OPEN_ERR         "error_cursor_open"
#define EMDB_CURSOR_IN_USE_ERR       "error_cursor_in_use"
#define EMDB_CURSOR_NO_CURSOR_ERR    "error_no_cursor"
#define EMDB_CURSOR_CLOSE_ERR        "error_cursor_close"
#define EMDB_CURSOR_GET_ERR          "error_cursor_get"
#define EMDB_CURSOR_SET_ERR          "error_cursor_set"
#define EMDB_CURSOR_DEL_ERR          "error_cursor_delete"
#define EMDB_SET_COMPARE_ERR         "error_set_compare"


/*
 * Error handling callbacks
 */

static void emdb_free (emdb_map_t * emdb_obj)
{
  FREE(emdb_obj);
}



static int erlcompare (const MDB_val *a, 
		       const MDB_val *b)
{
  ErlNifEnv * myenv = enif_alloc_env();

  ErlNifBinary key1 = {0};
  ErlNifBinary key2 = {0};
  
  int cmpval;

  key1.size = a -> mv_size;
  key1.data = a -> mv_data;

  key2.size = b -> mv_size;
  key2.data = b -> mv_data;
  
  
  cmpval = enif_compare(enif_make_binary(myenv, & key1), enif_make_binary(myenv, & key2));
  
  enif_free_env(myenv);
  
  return cmpval;
}

static ERL_NIF_TERM do_open (emdb_command * cmd, struct emdb_map_t *node)
{
  char *err;
  MDB_txn * txn;
  ERL_NIF_TERM handle;
  
  if (mdb_txn_begin(node -> env, NULL, 0, & txn))
    FAIL_FAST(EMDB_TXN_BEGIN_ERR, err2);
  
  if (mdb_open(txn, NULL, 0, & (node -> dbi)))
    FAIL_FAST(EMDB_OPEN_DBI_ERR, err1);
  
  /*the following line may be needed or not.... depends on your application needing erlang term order*/
  /*  if (mdb_set_compare(txn, node -> dbi, & erlcompare))
      FAIL_FAST(EMDB_SET_COMPARE_ERR, err1);
  */
  if (mdb_txn_commit(txn))
    FAIL_FAST(EMDB_TXN_COMMIT_ERR, err1);

  handle = enif_make_resource(cmd->env, node);
  enif_release_resource(node);
  
  return enif_make_tuple2(cmd->env,
			  enif_make_atom(cmd->env, "ok"),
			  handle);
  
 err1:
  mdb_txn_abort(txn);
  
 err2:
  mdb_env_close(node -> env);
 err3:
  emdb_free(node);
  
  return enif_make_tuple(cmd->env, 2, 
			 enif_make_atom(cmd->env, "error"),
			 enif_make_atom(cmd->env, err));  
}

static ERL_NIF_TERM do_close (emdb_command * cmd, struct emdb_map_t *node){
  if (NULL == node)
    return enif_make_atom(cmd->env, EMDB_INVALID_HANDLE_ERR);
  
  if (node -> dbi != NULL)
    mdb_close(node -> env, node -> dbi);
  
  mdb_env_close(node -> env);
 
  node->env = NULL;
  node->dbi = NULL;
  node->cursor = NULL;
  node->txn = NULL;

  
  //  enif_release_resource(node); //happens if the garbage-collector deletes the handle....
  
  return enif_make_atom(cmd->env, "ok");
}

static ERL_NIF_TERM do_put (emdb_command * cmd, struct emdb_map_t *node)
{
  ErlNifBinary key;
  ErlNifBinary val;

  MDB_val mkey;
  MDB_val mdata;

  MDB_txn * txn;
  char *err;
  int ret = 0;
  
  if(!node)
    FAIL_FAST("no_resource", err2);

  if (! enif_inspect_iolist_as_binary(cmd->env, cmd->args[0], &key))
    return enif_make_badarg(cmd->env);

  if (! enif_inspect_iolist_as_binary(cmd->env, cmd->args[1], &val))
    return enif_make_badarg(cmd->env);
  
  if(! node->dbi)
    FAIL_FAST("db_not_opened", err2);
  
  if(node -> txn == NULL) {
    if (mdb_txn_begin(node->env, NULL, 0, & txn))
      FAIL_FAST(EMDB_TXN_BEGIN_ERR, err2);
  } else {
    txn = node -> txn;
  }
  mkey.mv_size  = key.size;
  mkey.mv_data  = key.data;
  mdata.mv_size = val.size;
  mdata.mv_data = val.data;

  ret = mdb_put(txn, node -> dbi, & mkey, & mdata, MDB_NOOVERWRITE);

  if (MDB_KEYEXIST == ret)
    FAIL_FAST(EMDB_RET_KEY_EXIST, err1);

  if (MDB_TXN_FULL == ret)
    FAIL_FAST(EMDB_TXN_FULL_ERR, err1);

  if (ret)
    FAIL_FAST(EMDB_PUT_ERR, err1);
  
  
  if (node -> txn == NULL)
    if (mdb_txn_commit(txn))
      FAIL_FAST(EMDB_TXN_COMMIT_ERR, err1);
  
  return enif_make_atom(cmd->env, "ok");

 err1:

  if (node -> txn == NULL)
    mdb_txn_abort(txn);

 err2:
  
  return enif_make_tuple2(cmd->env, 
			  enif_make_atom(cmd->env, "error"),
			  enif_make_atom(cmd->env, err));
  
}



static ERL_NIF_TERM do_get (emdb_command * cmd, struct emdb_map_t *node)
{
  ErlNifBinary key;
  ErlNifBinary val = {0};
  ERL_NIF_TERM term;
  
  MDB_val mkey;
  MDB_val mdata;
  
  MDB_txn * txn;

  char * err;

  if (NULL == node)
    return enif_make_atom(cmd->env, EMDB_INVALID_HANDLE_ERR);

  if (! enif_inspect_iolist_as_binary(cmd->env, cmd->args[0], &key))
    return enif_make_badarg(cmd->env);

  mkey.mv_size  = key.size;
  mkey.mv_data  = key.data;
  
  if (node -> txn == NULL) {
    if (mdb_txn_begin(node->env, NULL, 0, & txn))
      FAIL_FAST(EMDB_TXN_BEGIN_ERR, err);
  } else txn = node -> txn;
  
  if(mdb_get(txn, node -> dbi, & mkey, & mdata))
    {
      if(node -> txn == NULL)
	mdb_txn_abort(txn);
      return enif_make_tuple2(cmd->env, 
			      enif_make_atom(cmd->env, "error"), 
			      enif_make_atom(cmd->env, "undefined"));
    }

  val.size = mdata.mv_size;
  val.data = mdata.mv_data;
  
  term = enif_make_binary(cmd->env, &val);
  if (node -> txn == NULL)
    mdb_txn_abort(txn);

  if (! term)
    FAIL_FAST(EMDB_MAKE_BINARY_ERR, err);

  return enif_make_tuple(cmd->env, 2,
                         enif_make_atom(cmd->env, "ok"),
                         term);
  
 err:
  
  return enif_make_tuple(cmd->env, 2, 
			 enif_make_atom(cmd->env, "error"),
			 enif_make_atom(cmd->env, err));
}

static ERL_NIF_TERM do_update (emdb_command * cmd, struct emdb_map_t *node){
  ErlNifBinary key;
  ErlNifBinary val;
  
  MDB_val mkey;
  MDB_val mdata;
  
  MDB_txn * txn;

  char * err;

  if (NULL == node)
    return enif_make_atom(cmd->env, EMDB_INVALID_HANDLE_ERR);

  if (! enif_inspect_iolist_as_binary(cmd->env, cmd->args[0], &key))
    return enif_make_badarg(cmd->env);

  if (! enif_inspect_iolist_as_binary(cmd->env, cmd->args[1], &val))
    return enif_make_badarg(cmd->env);

  if (node -> txn == NULL) {
    if (mdb_txn_begin(node->env, NULL, 0, & txn))
      FAIL_FAST(EMDB_TXN_BEGIN_ERR, err2);
  } else txn = node -> txn;
  
  mkey.mv_size  = key.size;
  mkey.mv_data  = key.data;
  mdata.mv_size = val.size;
  mdata.mv_data = val.data;

  if (mdb_put(txn, node -> dbi, & mkey, & mdata, 0))
    FAIL_FAST(EMDB_UPDATE_ERR, err1);

  if (node -> txn ==NULL)
    if (mdb_txn_commit(txn))
      FAIL_FAST(EMDB_TXN_COMMIT_ERR, err1);
  
  return enif_make_atom(cmd->env, "ok");

 err1:
  
  mdb_txn_abort(txn);
  
 err2:
  
  return enif_make_tuple2(cmd->env,
			  enif_make_atom(cmd->env, "error"),
			  enif_make_atom(cmd->env, err));
}

static ERL_NIF_TERM do_del (emdb_command * cmd, struct emdb_map_t *node){
  ErlNifBinary key;
  MDB_val mkey;
  MDB_txn * txn;
  char * err;
  int ret;
  
  if (NULL == node)
    return enif_make_atom(cmd->env, EMDB_INVALID_HANDLE_ERR);
  
  if (! enif_inspect_iolist_as_binary(cmd->env, cmd->args[0], &key))
    return enif_make_badarg(cmd->env);

  mkey.mv_size  = key.size;
  mkey.mv_data  = key.data;

  if (node -> txn == NULL) {
    if (mdb_txn_begin(node->env, NULL, 0, & txn))
      FAIL_FAST(EMDB_TXN_BEGIN_ERR, err);
  } else txn = node -> txn;
  
  ret = mdb_del(txn, node -> dbi, & mkey, NULL);
  
  if (node -> txn == NULL)
    if (mdb_txn_commit(txn))
      FAIL_FAST(EMDB_TXN_COMMIT_ERR, err);

  if(ret)
    return enif_make_atom(cmd->env, "undefined");
  
  return enif_make_atom(cmd->env, "ok");
  
 err:
  
  return enif_make_tuple(cmd->env, 2, 
			 enif_make_atom(cmd->env, "error"),
			 enif_make_atom(cmd->env, err));
}

static ERL_NIF_TERM do_append (emdb_command * cmd, struct emdb_map_t *node){
  ErlNifBinary key;
  ErlNifBinary val;

  MDB_val mkey;
  MDB_val mdata;

  MDB_txn * txn;

  char * err;
  int ret;

  if (NULL == node)
    return enif_make_atom(cmd->env, EMDB_INVALID_HANDLE_ERR);

  if (node -> txn == NULL) //either this or the commented behaviour beneath for appending outside transactions
    FAIL_FAST(EMDB_NO_TXN_ERR, err2);
  
  if (! enif_inspect_iolist_as_binary(cmd->env, cmd->args[0], &key))
    return enif_make_badarg(cmd->env);
  
  if (! enif_inspect_iolist_as_binary(cmd->env, cmd->args[1], &val))
    return enif_make_badarg(cmd->env);
  
  
  /*  if (mdb_txn_begin(handle, NULL, 0, & txn))
      FAIL_FAST(EMDB_TXN_BEGIN_ERR, err2);
  */
  mkey.mv_size  = key.size;
  mkey.mv_data  = key.data;
  mdata.mv_size = val.size;
  mdata.mv_data = val.data;
  
  ret = mdb_put(node -> txn, node -> dbi, & mkey, & mdata, MDB_APPEND);
  
  if (MDB_KEYEXIST == ret)
    FAIL_FAST(EMDB_RET_KEY_EXIST, err2);

  if (ret)
    FAIL_FAST(EMDB_PUT_ERR, err2);
  /*
  if (mdb_txn_commit(txn))
    FAIL_FAST(EMDB_TXN_COMMIT_ERR, err1);
  */
  return enif_make_atom(cmd->env, "ok");
 
 err2:
  
  return enif_make_tuple(cmd->env, 2, 
			 enif_make_atom(cmd->env, "error"),
			 enif_make_atom(cmd->env, err));
}

static ERL_NIF_TERM do_txn_begin (emdb_command * cmd, struct emdb_map_t *node){
  char * err;
  int ret;
  
  if (NULL == node)
    FAIL_FAST(EMDB_INVALID_HANDLE_ERR, err1);

  if (node -> cursor != NULL)
    FAIL_FAST(EMDB_CURSOR_IN_USE_ERR, err1);

  if (node -> txn != NULL)
    FAIL_FAST(EMDB_TXN_STARTED_ERR, err1);

  if (mdb_txn_begin(node->env, NULL, 0, & (node -> txn)))
    FAIL_FAST(EMDB_TXN_BEGIN_ERR, err1);

  return enif_make_atom(cmd->env, "ok");
  
 err1:

    return enif_make_tuple2(cmd->env,
			    enif_make_atom(cmd->env, "error"),
			    enif_make_atom(cmd->env, err));
}

static ERL_NIF_TERM do_txn_begin_ro (emdb_command * cmd, struct emdb_map_t *node){
  char * err;
  int ret;
  
  if (NULL == node)
    FAIL_FAST(EMDB_INVALID_HANDLE_ERR, err1);

  if (node -> cursor != NULL)
    FAIL_FAST(EMDB_CURSOR_IN_USE_ERR, err1);

  if (node -> txn != NULL)
    FAIL_FAST(EMDB_TXN_STARTED_ERR, err1);

  if (mdb_txn_begin(node->env, NULL, MDB_RDONLY, & (node -> txn)))
    FAIL_FAST(EMDB_TXN_BEGIN_ERR, err1);
  
  return enif_make_atom(cmd->env, "ok");
  
 err1:
  
  return enif_make_tuple2(cmd->env,
			  enif_make_atom(cmd->env, "error"),
			  enif_make_atom(cmd->env, err));
}

static ERL_NIF_TERM do_txn_commit (emdb_command * cmd, struct emdb_map_t *node){
  char * err;
  int ret;
  
  if (NULL == node)
    FAIL_FAST(EMDB_INVALID_HANDLE_ERR, err1);

  if (node -> txn == NULL)
    FAIL_FAST(EMDB_NO_TXN_ERR, err1);

  if (ret = mdb_txn_commit(node -> txn))
    FAIL_FAST(EMDB_TXN_COMMIT_ERR, err2);


  node -> cursor = NULL;
  node -> txn = NULL;

  return enif_make_atom(cmd->env, "ok");
  
 err1:
  return enif_make_tuple2(cmd->env,
			  enif_make_atom(cmd->env, "error"),
			  enif_make_atom(cmd->env, err));
  
 err2:
  return enif_make_tuple2(cmd->env, 
			  enif_make_atom(cmd->env, "error"),
			  enif_make_atom(cmd->env, err));
}

static ERL_NIF_TERM do_txn_abort (emdb_command * cmd, struct emdb_map_t *node){
  char * err;
  int ret;
  
  if (NULL == node)
    FAIL_FAST(EMDB_INVALID_HANDLE_ERR, err1);

  if (node -> txn == NULL)
    FAIL_FAST(EMDB_NO_TXN_ERR, err1);

  mdb_txn_abort(node -> txn);

  node -> cursor = NULL;
  node -> txn = NULL;
  
  return enif_make_atom(cmd->env, "ok");
  
 err1:
  return enif_make_tuple2(cmd->env,
			  enif_make_atom(cmd->env, "error"),
			  enif_make_atom(cmd->env, err));
  
}


static ERL_NIF_TERM do_drop (emdb_command * cmd, struct emdb_map_t *node){
  MDB_txn * txn;
  char * err;
  int ret;

  if (NULL == node)
    return enif_make_atom(cmd->env, EMDB_INVALID_HANDLE_ERR);

  if (mdb_txn_begin(node->env, NULL, 0, & txn))
    FAIL_FAST(EMDB_TXN_BEGIN_ERR, err2);

  ret = mdb_drop(txn, node -> dbi, 1);
  node -> dbi = NULL;

  if (ret)
    FAIL_FAST(EMDB_DROP_ERR, err1);

  if (mdb_txn_commit(txn))
    FAIL_FAST(EMDB_TXN_COMMIT_ERR, err1);

  return enif_make_atom(cmd->env, "ok");
  
 err1:
  mdb_txn_abort(txn);
  
 err2:
  
  return enif_make_tuple(cmd->env, 2, 
			 enif_make_atom(cmd->env, "error"),
			 enif_make_atom(cmd->env, err));
}


static ERL_NIF_TERM do_cursor_open (emdb_command * cmd, struct emdb_map_t *node){  
  char * err;
  int ret;
  
  if (NULL == node)
    FAIL_FAST(EMDB_INVALID_HANDLE_ERR, err1);

  if (node -> cursor != NULL)
    FAIL_FAST(EMDB_CURSOR_IN_USE_ERR, err1);

  if (node -> txn == NULL)
    if (mdb_txn_begin(node->env, NULL, 0, & (node -> txn)))
      FAIL_FAST(EMDB_TXN_BEGIN_ERR, err1);
  
  if( mdb_cursor_open(node -> txn, node -> dbi, &(node -> cursor)) )
    FAIL_FAST(EMDB_CURSOR_OPEN_ERR, err1);
  
  return enif_make_atom(cmd->env, "ok");
  
 err1:
 
  return enif_make_tuple2(cmd->env,
			  enif_make_atom(cmd->env, "error"),
			  enif_make_atom(cmd->env, err)); 
}

static ERL_NIF_TERM do_cursor_close (emdb_command * cmd, struct emdb_map_t *node){  
  char * err;
  int ret;
  
  if (NULL == node)
    FAIL_FAST(EMDB_INVALID_HANDLE_ERR, err1);

  if (node -> cursor == NULL)
    FAIL_FAST(EMDB_CURSOR_NO_CURSOR_ERR, err1);
  
  mdb_cursor_close(node -> cursor);
  
  node -> cursor = NULL;
  
  return enif_make_atom(cmd->env, "ok");
  
 err1:
  
  return enif_make_tuple2(cmd->env,
			  enif_make_atom(cmd->env, "error"),
			  enif_make_atom(cmd->env, err)); 
}

static ERL_NIF_TERM do_cursor_next (emdb_command * cmd, struct emdb_map_t *node){  
  char * err;

  MDB_val rkey;
  MDB_val rdata;

  ERL_NIF_TERM kterm;  
  ERL_NIF_TERM vterm;

  ErlNifBinary key = {0};
  ErlNifBinary val = {0};
  
  if (NULL == node)
    FAIL_FAST(EMDB_INVALID_HANDLE_ERR, err1);

  if (node -> txn == NULL)
    FAIL_FAST(EMDB_NO_TXN_ERR, err1);

  if (node -> cursor == NULL)
    FAIL_FAST(EMDB_CURSOR_NO_CURSOR_ERR, err1);
  
  if ( mdb_cursor_get(node -> cursor, &rkey, &rdata, MDB_NEXT) ) 
      FAIL_FAST(EMDB_CURSOR_GET_ERR, err1);
  
  /*  printf("key: %.*s, data: %.*s\n",
	 (int) rkey.mv_size,  (char *) rkey.mv_data,
	 (int) rdata.mv_size, (char *) rdata.mv_data);*/
  
  val.size = rdata.mv_size;
  val.data = rdata.mv_data;

  key.size = rkey.mv_size;
  key.data = rkey.mv_data;
  
  vterm = enif_make_binary(cmd->env, &val);
  kterm = enif_make_binary(cmd->env, &key);

  if (! vterm || ! kterm)
    FAIL_FAST(EMDB_MAKE_BINARY_ERR, err1);

  return enif_make_tuple2(cmd->env,
			  enif_make_atom(cmd->env, "ok"),
			  enif_make_tuple(cmd->env, 2, kterm, vterm));
  
 err1:

  return enif_make_tuple2(cmd->env,
			  enif_make_atom(cmd->env, "error"),
			  enif_make_atom(cmd->env, err));

}

static ERL_NIF_TERM do_cursor_set (emdb_command * cmd, struct emdb_map_t *node){  
  char * err;

  MDB_val rkey;
  MDB_val rdata;

  ERL_NIF_TERM kterm;  
  ERL_NIF_TERM vterm;

  ErlNifBinary key = {0};
  ErlNifBinary val = {0};
  
  if (NULL == node)
    FAIL_FAST(EMDB_INVALID_HANDLE_ERR, err1);
  
  if (node -> cursor == NULL)
    FAIL_FAST(EMDB_CURSOR_NO_CURSOR_ERR, err1);
  
  if (! enif_inspect_iolist_as_binary(cmd->env, cmd->args[0], &key))
    return enif_make_badarg(cmd->env);

  rkey.mv_size = key.size;
  rkey.mv_data = key.data;

  if ( mdb_cursor_get(node -> cursor, &rkey, &rdata, MDB_SET_KEY) ) 
    FAIL_FAST(EMDB_CURSOR_SET_ERR, err1);

  val.size = rdata.mv_size;
  val.data = rdata.mv_data;

  key.size = rkey.mv_size;
  key.data = rkey.mv_data;
  
  vterm = enif_make_binary(cmd->env, &val);
  kterm = enif_make_binary(cmd->env, &key);
  
  if (! vterm || ! kterm)
    FAIL_FAST(EMDB_MAKE_BINARY_ERR, err1);
  
  return enif_make_tuple(cmd->env, 2,
                         enif_make_atom(cmd->env, "ok"),
                         enif_make_tuple(cmd->env, 2, kterm, vterm));
  
 err1:
  
  return enif_make_tuple(cmd->env, 2, 
			 enif_make_atom(cmd->env, "error"),
			 enif_make_atom(cmd->env, err));
}

static ERL_NIF_TERM do_cursor_del (emdb_command * cmd, struct emdb_map_t *node){  
  char * err;
  
  ERL_NIF_TERM kterm;  
  ERL_NIF_TERM vterm;

  ErlNifBinary key = {0};
  ErlNifBinary val = {0};


  MDB_val rkey;
  MDB_val rdata;
  
  if (NULL == node)
    FAIL_FAST(EMDB_INVALID_HANDLE_ERR, err1);
  
  if (node -> cursor == NULL)
    FAIL_FAST(EMDB_CURSOR_NO_CURSOR_ERR, err1);
  
  if (node -> txn == NULL)
    FAIL_FAST(EMDB_NO_TXN_ERR, err1);

  if ( mdb_cursor_get(node -> cursor, &rkey, &rdata, MDB_GET_CURRENT) ) 
    FAIL_FAST(EMDB_CURSOR_GET_ERR, err1);

  if ( mdb_cursor_del(node -> cursor, 0) ) 
    FAIL_FAST(EMDB_CURSOR_DEL_ERR, err1);

  if ( mdb_cursor_get(node -> cursor, &rkey, &rdata, MDB_SET_RANGE) ) 
    FAIL_FAST(EMDB_CURSOR_SET_ERR, err1);
  
  val.size = rdata.mv_size;
  val.data = rdata.mv_data;
  
  key.size = rkey.mv_size;
  key.data = rkey.mv_data;
  
  vterm = enif_make_binary(cmd->env, &val);
  kterm = enif_make_binary(cmd->env, &key);

  if (! vterm || ! kterm)
    FAIL_FAST(EMDB_MAKE_BINARY_ERR, err1);

  return enif_make_tuple2(cmd->env,
			  enif_make_atom(cmd->env, "ok"),
			  enif_make_tuple(cmd->env, 2, kterm, vterm));

 err1:
  return enif_make_tuple2(cmd->env, 
			  enif_make_atom(cmd->env, "error"),
			  enif_make_atom(cmd->env, err));
}

static ERL_NIF_TERM do_dummy(emdb_command * cmd, struct emdb_map_t *node){  
  ERL_NIF_TERM rval;
  ErlDrvNowData *now = enif_alloc(sizeof(ErlDrvNowData));
  if(0<driver_get_now(now))
    return enif_make_tuple2(cmd->env, 
			    enif_make_atom(cmd->env, "error"),
			    enif_make_atom(cmd->env, "get_now_failed"));
  rval = enif_make_tuple2(cmd->env, 
			  enif_make_atom(cmd->env, "ok"),
			  enif_make_tuple3(cmd->env, 
					   enif_make_int(cmd->env, now->megasecs),
					   enif_make_int(cmd->env, now->secs),
					   enif_make_int(cmd->env, now->microsecs)));
  enif_free(now);
  return rval;
}

command_type atom_to_command_type(ErlNifEnv *env, ERL_NIF_TERM atom) {
  if(! enif_compare(atom, enif_make_atom(env, "get")))
    return cmd_get;
  if(! enif_compare(atom, enif_make_atom(env, "update")))
    return cmd_update;
  if(! enif_compare(atom, enif_make_atom(env, "del")))
    return cmd_del;
  return cmd_unknown;
}

static ERL_NIF_TERM evaluate_command(emdb_command *cmd, struct emdb_map_t *node);

static ERL_NIF_TERM do_bulk_txn(emdb_command * cmd, struct emdb_map_t *node){  
  ERL_NIF_TERM retval;
  
  ERL_NIF_TERM head; 
  ERL_NIF_TERM *tail;
  
  const ERL_NIF_TERM  *head_array;
  const ERL_NIF_TERM  *args;
  int args_arity;

  int arity;
  
  emdb_command current_command;
  
  char * err = "aborted";
  
  if(! enif_is_list(cmd->env, cmd->args[0]))
    return enif_make_badarg(cmd->env);
  
  if( node->txn == NULL){
    if (mdb_txn_begin(node->env, NULL, 0, & (node -> txn)))
      FAIL_FAST(EMDB_TXN_BEGIN_ERR, err1);
  }
  
  retval = enif_make_list(cmd->env, 0);
  
  tail = &(cmd->args[0]);
  
  while(!enif_is_empty_list(cmd->env, *tail)){ 
    
    if(! enif_get_list_cell(cmd->env, *tail, &head, tail))
      FAIL_FAST("list_get_error", err2);
    
    if( ! enif_is_tuple(cmd->env, head))
      FAIL_FAST("list_elem_no_tuple", err2);
    
    if(! enif_get_tuple(cmd->env, head, &arity, &head_array))
      FAIL_FAST("list_elem_no_tuple", err2);
    
    if(arity != 2)
      FAIL_FAST("command_format", err2);
    
    if(! enif_get_tuple(cmd->env, head_array[1], &args_arity, &args))
      FAIL_FAST("args_no_tuple", err2);
    
    current_command.env = cmd->env;
    current_command.args = args;
    current_command.type = atom_to_command_type(cmd->env, head_array[0]);
    
    retval = enif_make_list_cell(cmd->env, evaluate_command(&current_command, node), retval);
  }
  
  if (mdb_txn_commit(node -> txn))
    FAIL_FAST(EMDB_TXN_COMMIT_ERR, err2);

  node->txn = NULL;
  //node->txn = store_txn;
  
  if(! enif_make_reverse_list(cmd->env, retval, &retval))
    FAIL_FAST("list_reverse_error", err1);
  
  return enif_make_tuple2(cmd->env, 
			  enif_make_atom(cmd->env, "ok"),
			  retval);
  
 err2:
  mdb_txn_abort(node->txn);
  node->txn = NULL;
  //node->txn = store_txn;
  
 err1:
  return enif_make_tuple2(cmd->env, 
			  enif_make_atom(cmd->env, "error"),
			  enif_make_atom(cmd->env, err));
}

static ERL_NIF_TERM evaluate_command(emdb_command *cmd, struct emdb_map_t *node)
{
  char * err;
  
  switch(cmd->type) {
  case cmd_open:
    return do_open(cmd, node);
  case cmd_close:
    return do_close(cmd, node);
  case cmd_put:
    return do_put(cmd, node);
  case cmd_get:
    return do_get(cmd, node);
  case cmd_del:
    return do_del(cmd, node);
  case cmd_append:
    return do_append(cmd, node);
  case cmd_txn_begin:
    return do_txn_begin(cmd, node);
  case cmd_txn_begin_ro:
    return do_txn_begin_ro(cmd, node);
  case cmd_txn_commit:
    return do_txn_commit(cmd, node);
  case cmd_txn_abort:
    return do_txn_abort(cmd, node);
  case cmd_drop:
    return do_drop(cmd, node);
  case cmd_update:
    return do_update(cmd, node);
  case cmd_cursor_open:
    return do_cursor_open(cmd, node);
  case cmd_cursor_close:
    return do_cursor_close(cmd, node);
  case cmd_cursor_next:
    return do_cursor_next(cmd, node);
  case cmd_cursor_set:
    return do_cursor_set(cmd, node);
  case cmd_cursor_del:
    return do_cursor_del(cmd, node);
  case cmd_dummy:
    return do_dummy(cmd, node);
  case cmd_bulk_txn:
    return do_bulk_txn(cmd, node);
  default:
    FAIL_FAST("invalid_command", err1);
  }
  
 err1:
  
  return enif_make_tuple(cmd->env, 2, 
			 enif_make_atom(cmd->env, "error"),
			 enif_make_atom(cmd->env, err));
}


static void command_destroy(void *obj) 
{
    emdb_command *cmd = (emdb_command *) obj;

    if(cmd->env != NULL) 
	   enif_free_env(cmd->env);

    enif_free(cmd->args);
    enif_free(cmd);
}
/*
static void command_requeue(void *obj, emdb_map_t *node) 
{
    emdb_command *cmd = (emdb_command *) obj;

    if(cmd->env != NULL) {
      enif_clear_env(cmd->env);
      queue_push(node->env_queue, cmd->env); 
    }
    
    enif_free(cmd->args);
    enif_free(cmd);
}
*/
static emdb_command * command_create(emdb_map_t *node) 
{
  MDB_env * env;
  
  env = enif_alloc_env();
  
  emdb_command *cmd = (emdb_command *) enif_alloc(sizeof(emdb_command));
  if(cmd == NULL)
    return NULL;
  
  
  cmd->env = env;
  if(cmd->env == NULL) {
    command_destroy(cmd);
    return NULL;
  }
  
  cmd->type = cmd_unknown;
  cmd->ref = 0;
  cmd->args = NULL;
  
  return cmd;
}


static void * emdb_worker_run(void *arg)
{
  
  struct emdb_map_t * node;
  emdb_command * cmd;
  node = (struct emdb_map_t *) arg;
  int continue_running = 1;
  
  while(continue_running) {
    cmd = queue_pop(node->command_queue);
    
    if(cmd->type == cmd_stop) 
      continue_running = 0;
    else { 
      
      enif_send(NULL, &cmd->pid, cmd->env, enif_make_tuple2(cmd->env, 
							    cmd->ref, 
							    evaluate_command(cmd, node)));
    }
    command_destroy(cmd);
  }
  
  return NULL;
}

static ERL_NIF_TERM * copy_args (ErlNifEnv * env,
				 int argc, const ERL_NIF_TERM argv[])
{
  ERL_NIF_TERM *retval;
  int i = 0;

  retval = (ERL_NIF_TERM *) enif_alloc(sizeof(ERL_NIF_TERM) * argc);
  
  if (!retval) 
    return NULL;

  while(i < argc) {
    *(retval + i) = enif_make_copy(env, argv[i]);
    i = i+1;
  }
  return retval;
}


static ERL_NIF_TERM push_command(ErlNifEnv *env, struct emdb_map_t *node, emdb_command *cmd) {
  char *err;
  
  if(!queue_push(node->command_queue, cmd)) 
    FAIL_FAST("command_push_failed", err1);
  
  return enif_make_atom(env, "ok");
  
 err1:
  return enif_make_tuple(env, 2, 
			 enif_make_atom(cmd->env, "error"),
			 enif_make_atom(env, err));
}

static void destruct_node(ErlNifEnv *env, void *arg)
{
    struct emdb_map_t *node = (struct emdb_map_t *) arg;
    emdb_command *cmd = command_create(node);
  
    /* Send the stop command 
     */

    if (!node->command_queue)
      return;
    cmd->type = cmd_stop;
    queue_push(node->command_queue, cmd);

     
    /* Wait for the thread to finish 
     */
    enif_thread_join(node->tid, NULL);
    enif_thread_opts_destroy(node->opts);
     
    /* The thread has finished... now remove the command queue, and close
     * the datbase (if it was still open).
     */
    queue_destroy(node->command_queue);

    node->command_queue=NULL;

    //    if(node->dbi)
    //	   sqlite3_close(db->db);
}



static ERL_NIF_TERM relay_command (ErlNifEnv * env,
                                   int argc, 
				   const ERL_NIF_TERM argv[],
				   command_type command_t)
{
  emdb_command *cmd;

  struct emdb_map_t * node;
  char * err;
  int ret;
  ErlNifPid pid;
  
  if(!enif_get_resource(env, argv[1], emdb_map_type, (void **) &node))
    FAIL_FAST("invalid_resource", err2);
  
  if (!node)
    return enif_make_atom(env, EMDB_INVALID_HANDLE_ERR);
  
  if (!node -> env) 
    return enif_make_tuple2(env, 
			    enif_make_atom(env, "error"),
			    enif_make_atom(env, "no_environment"));
      
  if(!enif_self(env, &pid)) 
    FAIL_FAST("invalid_pid", err2);

  if(!enif_is_ref(env, argv[0]))
    FAIL_FAST("first_arg_no_ref", err2);
  
  if(! node->command_queue) 
    FAIL_FAST("no_command_queue" ,err2);
 
  cmd = command_create(node);
  if(!cmd) 
    FAIL_FAST("failed_to_create_command", err2);

  cmd->type = command_t;
  cmd->ref = enif_make_copy(cmd->env, argv[0]);
  cmd->pid = pid;
  cmd->args = copy_args(cmd->env, argc - 2, &argv[2]); 

  push_command(env, node, cmd);

  return enif_make_atom(env, "ok");
  
 err2:
  
  return enif_make_tuple2(env, 
			  enif_make_atom(env, "error"),
			  enif_make_atom(env, err));
}


/*
 * Driver callbacks
 */


static ERL_NIF_TERM emdb_open_nif (ErlNifEnv * env,
                                   int argc, const ERL_NIF_TERM argv[])
{
  char dirname [MAXPATHLEN];
  struct emdb_map_t *node;

  char * err;
  ErlNifUInt64 mapsize;
  ErlNifUInt64 envflags;
  emdb_command * cmd = NULL;
  ErlNifPid pid;
  
      
  if (enif_get_string(env, argv[1], dirname, MAXPATHLEN, ERL_NIF_LATIN1) <= 0)
    return enif_make_badarg(env);
  
  node = enif_alloc_resource(emdb_map_type, sizeof(struct emdb_map_t));  
  node->txn = NULL;
  node->cursor = NULL;

  if(!node)
    FAIL_FAST(EMDB_MALLOC_ERR, err3);


  /* Create command queue */
  node->command_queue = queue_create();
  if(!node->command_queue) {
    //enif_release_resource(conn);
    FAIL_FAST("command_queue_create_failed", err2);
  }

  node->opts = enif_thread_opts_create("emdb_thread_opts");
  if(enif_thread_create("emdb_worker", &node->tid, emdb_worker_run, node, node->opts) != 0)
    FAIL_FAST("thread_create_failed", err2);
  
  
  if (mdb_env_create(& (node -> env)))
    FAIL_FAST(EMDB_CREATE_ERR, err2);
  
  if (! enif_get_uint64(env, argv[2], & mapsize))
    return enif_make_badarg(env);

  if (mdb_env_set_mapsize(node -> env, mapsize))
    FAIL_FAST(EMDB_MAPSIZE_ERR, err2);
      
  if (! enif_get_uint64(env, argv[3], & envflags))
    return enif_make_badarg(env);

  if(!enif_self(env, &pid)) 
    FAIL_FAST("invalid_pid", err3);

  if(!enif_is_ref(env, argv[0]))
    FAIL_FAST("first_arg_no_ref", err3);
  
  if (mdb_env_open(node -> env, dirname, envflags, 0664))
    FAIL_FAST(EMDB_OPEN_ERR, err2);
  

  
  cmd = command_create(node);
  if(!cmd) 
    FAIL_FAST("failed_to_create_command", err2);

  cmd->type = cmd_open;
  cmd->ref = enif_make_copy(cmd->env, argv[0]);
  cmd->pid = pid;
  cmd->args = copy_args(cmd->env, argc - 3, &argv[3]); 

  push_command(env, node, cmd);

  return enif_make_atom(env, "ok");
  
 err2:
  mdb_env_close(node -> env);
 err3:
  
  enif_release_resource(node);

  return enif_make_tuple(env, 2, 
			 enif_make_atom(cmd->env, "error"),
			 enif_make_atom(env, err));
}


static ERL_NIF_TERM emdb_close_nif (ErlNifEnv * env,
                                    int argc, const ERL_NIF_TERM argv[])
{
  return relay_command(env, argc, argv, cmd_close);
}


static ERL_NIF_TERM emdb_txn_begin_nif (ErlNifEnv * env,
						int argc, const ERL_NIF_TERM argv[])
{
  return relay_command(env, argc, argv, cmd_txn_begin);
}

static ERL_NIF_TERM emdb_txn_begin_ro_nif (ErlNifEnv * env,
						int argc, const ERL_NIF_TERM argv[])
{
  return relay_command(env, argc, argv, cmd_txn_begin_ro);
}

static ERL_NIF_TERM emdb_txn_commit_nif (ErlNifEnv * env,
					 int argc, const ERL_NIF_TERM argv[])
{
  return relay_command(env, argc, argv, cmd_txn_commit);
}


static ERL_NIF_TERM emdb_txn_abort_nif (ErlNifEnv * env,
					 int argc, const ERL_NIF_TERM argv[])
{
  return relay_command(env, argc, argv, cmd_txn_abort);
}

static ERL_NIF_TERM emdb_put_nif (ErlNifEnv * env,
                                  int argc, const ERL_NIF_TERM argv[])
{
  return relay_command(env, argc, argv, cmd_put);
}


//appends happen to the end of the db, incorrect order leads to inconsistency

static ERL_NIF_TERM emdb_append_nif (ErlNifEnv * env,
                                  int argc, const ERL_NIF_TERM argv[])
{
  return relay_command(env, argc, argv, cmd_append);  
}


static ERL_NIF_TERM emdb_get_nif (ErlNifEnv * env,
                                  int argc, const ERL_NIF_TERM argv[])
{
  return relay_command(env, argc, argv, cmd_get);  
}


static ERL_NIF_TERM emdb_del_nif (ErlNifEnv * env,
                                  int argc, const ERL_NIF_TERM argv[])
{
  return relay_command(env, argc, argv, cmd_del);  
}


static ERL_NIF_TERM emdb_update_nif (ErlNifEnv * env,
                                     int argc, const ERL_NIF_TERM argv[])
{
  return relay_command(env, argc, argv, cmd_update);
}



static ERL_NIF_TERM emdb_drop_nif (ErlNifEnv * env,
                                   int argc, const ERL_NIF_TERM argv[])
{
  return relay_command(env, argc, argv, cmd_drop);
  
 }




static ERL_NIF_TERM emdb_cursor_open_nif (ErlNifEnv * env,
					  int argc, const ERL_NIF_TERM argv[])
{
  return relay_command(env, argc, argv, cmd_cursor_open);
}




static ERL_NIF_TERM emdb_cursor_close_nif (ErlNifEnv * env,
					   int argc, const ERL_NIF_TERM argv[])
{
  return relay_command(env, argc, argv, cmd_cursor_close);
}




static ERL_NIF_TERM emdb_cursor_next_nif (ErlNifEnv * env,
					   int argc, const ERL_NIF_TERM argv[])
{
  return relay_command(env, argc, argv, cmd_cursor_next);
}



static ERL_NIF_TERM emdb_cursor_set_nif (ErlNifEnv * env,
					   int argc, const ERL_NIF_TERM argv[])
{
  return relay_command(env, argc, argv, cmd_cursor_set);
}

static ERL_NIF_TERM emdb_cursor_del_nif (ErlNifEnv * env,
					   int argc, const ERL_NIF_TERM argv[])
{
  return relay_command(env, argc, argv, cmd_cursor_del);
}

static ERL_NIF_TERM emdb_dummy_action_nif (ErlNifEnv * env,
					   int argc, const ERL_NIF_TERM argv[])
{
  return relay_command(env, argc, argv, cmd_dummy);
}

static ERL_NIF_TERM emdb_bulk_txn_nif(ErlNifEnv * env,
				      int argc, const ERL_NIF_TERM argv[]){
  return relay_command(env, argc, argv, cmd_bulk_txn);
}

static int emdb_load(ErlNifEnv* env, void** priv, ERL_NIF_TERM load_info)
  {
    ErlNifResourceType *rt;

    rt = enif_open_resource_type(env, "emdb_drv", "emdb_map_type", 
				destruct_node, ERL_NIF_RT_CREATE, NULL);
    
    if(!rt) 
	    return -1;
    emdb_map_type = rt;

    return (0);
  }

static int emdb_reload(ErlNifEnv* env, void** priv, ERL_NIF_TERM info)
{
  return (0);
}
  

static int emdb_upgrade(ErlNifEnv* env, void** priv, void** old_priv, ERL_NIF_TERM load_info)
{
  return (0);
}


static void emdb_unload(ErlNifEnv* env, void* priv)
{
  return;
}



static ErlNifFunc nif_funcs [] = {
  {"open",        4, emdb_open_nif},
  {"close",       2, emdb_close_nif},
  {"put",         4, emdb_put_nif},
  {"append",      4, emdb_append_nif},
  {"get",         3, emdb_get_nif},
  {"del",         3, emdb_del_nif},
  {"update",      4, emdb_update_nif},
  {"drop",        2, emdb_drop_nif},
  {"cursor_open", 2, emdb_cursor_open_nif},
  {"cursor_close",2, emdb_cursor_close_nif},
  {"cursor_next", 2, emdb_cursor_next_nif},
  {"cursor_set",  3, emdb_cursor_set_nif},
  {"cursor_del",  2, emdb_cursor_del_nif},
  {"txn_begin",   2, emdb_txn_begin_nif},
  {"txn_begin_ro",2, emdb_txn_begin_ro_nif},
  {"txn_commit",  2, emdb_txn_commit_nif},
  {"txn_abort",   2, emdb_txn_abort_nif},
  {"dummy_action",2, emdb_dummy_action_nif},
  {"bulk_txn"    ,3, emdb_bulk_txn_nif}};

/* driver entry point */
ERL_NIF_INIT(emdb_drv,
             nif_funcs,
             & emdb_load,
             & emdb_reload,
             & emdb_upgrade,
             & emdb_unload)
