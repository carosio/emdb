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
#include <uthash.h>     /* for uthash */
#include <lmdb.h>        /* for MDB interface */



#define FREE(p)   (NULL == (p) ? 0 : (free(p), p = NULL))

#define FAIL_FAST(Error, Goto)                    \
  do{                                             \
    err = Error;                                  \
    goto Goto;                                    \
}while(0)


struct emdb_map_t {
  MDB_env * env;
  MDB_dbi   dbi;
  MDB_cursor * cursor;
  MDB_txn * txn;  

  UT_hash_handle  hh;
};


static ERL_NIF_TERM atom_ok;
static ERL_NIF_TERM atom_undefined;
static ERL_NIF_TERM atom_error;

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

static void emdb_free (struct emdb_map_t * emdb_obj)
{
  FREE(emdb_obj);
}



static int erlcompare (const MDB_val *a, 
		       const MDB_val *b)
{
  ErlNifEnv * myenv = enif_alloc_env();

  ErlNifBinary key1 = {0};
  ErlNifBinary key2 = {0};
  
  key1.size = a -> mv_size;
  key1.data = a -> mv_data;

  key2.size = b -> mv_size;
  key2.data = b -> mv_data;
  

  return enif_compare(enif_make_binary(myenv, & key1), enif_make_binary(myenv, & key2));
}

/*
 * Driver callbacks
 */

static ERL_NIF_TERM emdb_open_nif (ErlNifEnv * env,
                                   int argc, const ERL_NIF_TERM argv[])
{
  char dirname [MAXPATHLEN];
  struct emdb_map_t * node;
  MDB_txn * txn;
  char * err;
  ErlNifUInt64 mapsize;
  ErlNifUInt64 envflags;

  if (enif_get_string(env, argv[0], dirname, MAXPATHLEN, ERL_NIF_LATIN1) <= 0)
    return enif_make_badarg(env);
  
  if(! (node = calloc(1, sizeof(struct emdb_map_t))))
    FAIL_FAST(EMDB_MALLOC_ERR, err3);
  
  if (mdb_env_create(& (node -> env)))
    FAIL_FAST(EMDB_CREATE_ERR, err2);
  
  if (! enif_get_uint64(env, argv[1], & mapsize))
    return enif_make_badarg(env);

  if (mdb_env_set_mapsize(node -> env, mapsize))
    FAIL_FAST(EMDB_MAPSIZE_ERR, err2);
      
  if (! enif_get_uint64(env, argv[2], & envflags))
    return enif_make_badarg(env);

  if (mdb_env_open(node -> env, dirname, envflags, 0664))
    FAIL_FAST(EMDB_OPEN_ERR, err2);

  if (mdb_txn_begin(node -> env, NULL, 0, & txn))
    FAIL_FAST(EMDB_TXN_BEGIN_ERR, err2);

  if (mdb_open(txn, NULL, 0, & (node -> dbi)))
    FAIL_FAST(EMDB_OPEN_DBI_ERR, err1);
  
  /*  if (mdb_set_compare(txn, node -> dbi, & erlcompare))
    FAIL_FAST(EMDB_SET_COMPARE_ERR, err1);
  */
  if (mdb_txn_commit(txn))
    FAIL_FAST(EMDB_TXN_COMMIT_ERR, err1);
  
  HASH_ADD_PTR(emdb_map, env, node);
  
  return enif_make_tuple(env, 2,
			 atom_ok,
			 enif_make_ulong(env, (unsigned long) node -> env));
  
 err1:
  mdb_txn_abort(txn);
 err2:
  mdb_env_close(node -> env);
 err3:
  emdb_free(node);

  return enif_make_tuple(env, 2, 
			 atom_error,
			 enif_make_atom(env, err));
}

static ERL_NIF_TERM emdb_close_nif (ErlNifEnv * env,
                                    int argc, const ERL_NIF_TERM argv[])
{
  MDB_env * handle;
  struct emdb_map_t * node;
  unsigned long addr;

  printf("parsing the handle\n");

  if (! enif_get_ulong(env, argv[0], & addr))
    return enif_make_badarg(env);
  
  handle = (MDB_env *) addr;

  printf("pulling the handle out of the hashmap\n");

  HASH_FIND_PTR(emdb_map, & handle, node);
  if (NULL == node)
    return enif_make_atom(env, EMDB_INVALID_HANDLE_ERR);

  printf("deleting the hashmap entry\n");

  HASH_DEL(emdb_map, node);

  printf("closing the handle\n");

  mdb_env_close(handle);
  emdb_free(node);

  return atom_ok;
 }


static ERL_NIF_TERM emdb_txn_begin_nif (ErlNifEnv * env,
						int argc, const ERL_NIF_TERM argv[])
{
  MDB_env * handle;
  struct emdb_map_t * node;
  unsigned long addr;
  char * err;
  int ret;
  
  if (! enif_get_ulong(env, argv[0], & addr))
    return enif_make_badarg(env);
  
  handle = (MDB_env *) addr;

  HASH_FIND_PTR(emdb_map, & handle, node);
  if (NULL == node)
    FAIL_FAST(EMDB_INVALID_HANDLE_ERR, err1);

  if (node -> cursor != NULL)
    FAIL_FAST(EMDB_CURSOR_IN_USE_ERR, err1);

  if (node -> txn != NULL)
    FAIL_FAST(EMDB_TXN_STARTED_ERR, err1);

  if (mdb_txn_begin(handle, NULL, 0, & (node -> txn)))
    FAIL_FAST(EMDB_TXN_BEGIN_ERR, err1);

  return atom_ok;
  
 err1:

    return enif_make_tuple(env, 2, 
			 atom_error,
			 enif_make_atom(env, err));
}

static ERL_NIF_TERM emdb_txn_begin_ro_nif (ErlNifEnv * env,
						int argc, const ERL_NIF_TERM argv[])
{
  MDB_env * handle;
  struct emdb_map_t * node;
  unsigned long addr;
  char * err;
  int ret;
  
  if (! enif_get_ulong(env, argv[0], & addr))
    return enif_make_badarg(env);
  
  handle = (MDB_env *) addr;

  HASH_FIND_PTR(emdb_map, & handle, node);
  if (NULL == node)
    FAIL_FAST(EMDB_INVALID_HANDLE_ERR, err1);

  if (node -> cursor != NULL)
    FAIL_FAST(EMDB_CURSOR_IN_USE_ERR, err1);

  if (node -> txn != NULL)
    FAIL_FAST(EMDB_TXN_STARTED_ERR, err1);

  if (mdb_txn_begin(handle, NULL, MDB_RDONLY, & (node -> txn)))
    FAIL_FAST(EMDB_TXN_BEGIN_ERR, err1);

  return atom_ok;
  
 err1:
  
  return enif_make_tuple(env, 2, 
			 atom_error,
			 enif_make_atom(env, err));
}

static ERL_NIF_TERM emdb_txn_commit_nif (ErlNifEnv * env,
					 int argc, const ERL_NIF_TERM argv[])
{
  MDB_env * handle;
  struct emdb_map_t * node;
  unsigned long addr;
  char * err;
  int ret;
  
  if (! enif_get_ulong(env, argv[0], & addr))
    return enif_make_badarg(env);
  
  handle = (MDB_env *) addr;

  HASH_FIND_PTR(emdb_map, & handle, node);
  if (NULL == node)
    FAIL_FAST(EMDB_INVALID_HANDLE_ERR, err1);

  if (node -> txn == NULL)
    FAIL_FAST(EMDB_NO_TXN_ERR, err1);

  if (ret = mdb_txn_commit(node -> txn))
    FAIL_FAST(EMDB_TXN_COMMIT_ERR, err2);


  node -> cursor = NULL;
  node -> txn = NULL;

  return atom_ok;
  
 err1:
  return enif_make_tuple(env, 2, 
			 atom_error,
			 enif_make_atom(env, err));
  
 err2:
  return enif_make_tuple(env, 2, 
			 atom_error,
			 enif_make_atom(env, err),
			 enif_make_int(env, ret));
			 
}




static ERL_NIF_TERM emdb_put_nif (ErlNifEnv * env,
                                  int argc, const ERL_NIF_TERM argv[])
{
  ErlNifBinary key;
  ErlNifBinary val;

  MDB_val mkey;
  MDB_val mdata;

  MDB_env * handle;
  MDB_txn * txn;

  struct emdb_map_t * node;
  unsigned long addr;
  char * err;
  int ret;

  if (! enif_get_ulong(env, argv[0], & addr))
    return enif_make_badarg(env);
  
  handle = (MDB_env *) addr;

  HASH_FIND_PTR(emdb_map, & handle, node);
  if (NULL == node)
    return enif_make_atom(env, EMDB_INVALID_HANDLE_ERR);

  if (! enif_inspect_iolist_as_binary(env, argv[1], &key))
    return enif_make_badarg(env);

  if (! enif_inspect_iolist_as_binary(env, argv[2], &val))
    return enif_make_badarg(env);
  
  if(node -> txn == NULL) {
    if (mdb_txn_begin(handle, NULL, 0, & txn))
      FAIL_FAST(EMDB_TXN_BEGIN_ERR, err2);
  } else 
    txn = node -> txn;
  
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
  
  return atom_ok;

 err1:

  if (node -> txn == NULL)
    mdb_txn_abort(txn);

 err2:

  return enif_make_tuple(env, 2, 
			 atom_error,
			 enif_make_atom(env, err));

}


//appends happen to the end of the db, incorrect order leads to inconsistency

static ERL_NIF_TERM emdb_append_nif (ErlNifEnv * env,
                                  int argc, const ERL_NIF_TERM argv[])
{
  ErlNifBinary key;
  ErlNifBinary val;

  MDB_val mkey;
  MDB_val mdata;

  MDB_env * handle;
  MDB_txn * txn;

  struct emdb_map_t * node;
  unsigned long addr;
  char * err;
  int ret;

  if (! enif_get_ulong(env, argv[0], & addr))
    return enif_make_badarg(env);
  
  handle = (MDB_env *) addr;

  HASH_FIND_PTR(emdb_map, & handle, node);
  if (NULL == node)
    return enif_make_atom(env, EMDB_INVALID_HANDLE_ERR);

  if (node -> txn == NULL)
    FAIL_FAST(EMDB_NO_TXN_ERR, err2);
  
  if (! enif_inspect_iolist_as_binary(env, argv[1], &key))
    return enif_make_badarg(env);
  
  if (! enif_inspect_iolist_as_binary(env, argv[2], &val))
    return enif_make_badarg(env);
  
  
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
  return atom_ok;
 
 err2:
  
  return enif_make_tuple(env, 2, 
			 atom_error,
			 enif_make_atom(env, err));

}


static ERL_NIF_TERM emdb_get_nif (ErlNifEnv * env,
                                  int argc, const ERL_NIF_TERM argv[])
{

  ErlNifBinary key;
  ErlNifBinary val = {0};
  ERL_NIF_TERM term;

  MDB_val mkey;
  MDB_val mdata;

  MDB_env * handle;
  MDB_txn * txn;

  struct emdb_map_t * node;
  char * err;
  unsigned long addr;

  if (! enif_get_ulong(env, argv[0], & addr))
    return enif_make_badarg(env);
  
  handle = (MDB_env *) addr;

  HASH_FIND_PTR(emdb_map, & handle, node);

  if (NULL == node)
    return enif_make_atom(env, EMDB_INVALID_HANDLE_ERR);

  if (! enif_inspect_iolist_as_binary(env, argv[1], &key))
    return enif_make_badarg(env);

  mkey.mv_size  = key.size;
  mkey.mv_data  = key.data;
  
  if (node -> txn == NULL) {
    if (mdb_txn_begin(handle, NULL, 0, & txn))
      FAIL_FAST(EMDB_TXN_BEGIN_ERR, err);
  } else txn = node -> txn;
  
  if(mdb_get(txn, node -> dbi, & mkey, & mdata))
    {
      mdb_txn_abort(txn);
      return atom_undefined;
    }

  val.size = mdata.mv_size;
  val.data = mdata.mv_data;
  
  term = enif_make_binary(env, &val);
  if (node -> txn == NULL)
    mdb_txn_abort(txn);

  if (! term)
    FAIL_FAST(EMDB_MAKE_BINARY_ERR, err);

  return enif_make_tuple(env, 2,
                         atom_ok,
                         term);

 err:
  
  return enif_make_tuple(env, 2, 
			 atom_error,
			 enif_make_atom(env, err));
}


static ERL_NIF_TERM emdb_del_nif (ErlNifEnv * env,
                                  int argc, const ERL_NIF_TERM argv[])
{
  ErlNifBinary key;

  MDB_val mkey;

  MDB_env * handle;
  MDB_txn * txn;

  struct emdb_map_t * node;
  char * err;
  unsigned long addr;
  int ret;

  if (! enif_get_ulong(env, argv[0], & addr))
    return enif_make_badarg(env);
  
  handle = (MDB_env *) addr;

  HASH_FIND_PTR(emdb_map, & handle, node);
  if (NULL == node)
    return enif_make_atom(env, EMDB_INVALID_HANDLE_ERR);

  if (! enif_inspect_iolist_as_binary(env, argv[1], &key))
    return enif_make_badarg(env);

  mkey.mv_size  = key.size;
  mkey.mv_data  = key.data;
  
  if (node -> txn == NULL) {
    if (mdb_txn_begin(handle, NULL, 0, & txn))
      FAIL_FAST(EMDB_TXN_BEGIN_ERR, err);
  } else txn = node -> txn;
  
  ret = mdb_del(txn, node -> dbi, & mkey, NULL);
  
  if (node -> txn == NULL)
    if (mdb_txn_commit(txn))
      FAIL_FAST(EMDB_TXN_COMMIT_ERR, err);

  if(ret)
    return atom_undefined;
  
  return atom_ok;
  
 err:
  
  return enif_make_tuple(env, 2, 
			 atom_error,
			 enif_make_atom(env, err));

}


static ERL_NIF_TERM emdb_update_nif (ErlNifEnv * env,
                                     int argc, const ERL_NIF_TERM argv[])
{
  ErlNifBinary key;
  ErlNifBinary val;

  MDB_val mkey;
  MDB_val mdata;

  MDB_env * handle;
  MDB_txn * txn;

  struct emdb_map_t * node;
  unsigned long addr;
  char * err;

  if (! enif_get_ulong(env, argv[0], & addr))
    return enif_make_badarg(env);
  
  handle = (MDB_env *) addr;

  HASH_FIND_PTR(emdb_map, & handle, node);
  if (NULL == node)
    return enif_make_atom(env, EMDB_INVALID_HANDLE_ERR);

  if (! enif_inspect_iolist_as_binary(env, argv[1], &key))
    return enif_make_badarg(env);

  if (! enif_inspect_iolist_as_binary(env, argv[2], &val))
    return enif_make_badarg(env);

  if (node -> txn == NULL) {
    if (mdb_txn_begin(handle, NULL, 0, & txn))
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
  
  return atom_ok;

 err1:

  mdb_txn_abort(txn);

 err2:
 
  return enif_make_tuple(env, 2, 
			 atom_error,
			 enif_make_atom(env, err));
}



static ERL_NIF_TERM emdb_drop_nif (ErlNifEnv * env,
                                   int argc, const ERL_NIF_TERM argv[])
{
  MDB_env * handle;
  MDB_txn * txn;
  struct emdb_map_t * node;
  unsigned long addr;
  char * err;
  int ret;

  if (! enif_get_ulong(env, argv[0], & addr))
    return enif_make_badarg(env);
  
  handle = (MDB_env *) addr;

  HASH_FIND_PTR(emdb_map, & handle, node);
  if (NULL == node)
    return enif_make_atom(env, EMDB_INVALID_HANDLE_ERR);

  if (mdb_txn_begin(handle, NULL, 0, & txn))
    FAIL_FAST(EMDB_TXN_BEGIN_ERR, err2);

  ret = mdb_drop(txn, node -> dbi, 0);
  if (ret)
    FAIL_FAST(EMDB_DROP_ERR, err1);

  if (mdb_txn_commit(txn))
    FAIL_FAST(EMDB_TXN_COMMIT_ERR, err1);

  return atom_ok;

 err1:
  mdb_txn_abort(txn);
  
 err2:
  
  return enif_make_tuple(env, 2, 
			 atom_error,
			 enif_make_atom(env, err));
 }




static ERL_NIF_TERM emdb_cursor_open_nif (ErlNifEnv * env,
					  int argc, const ERL_NIF_TERM argv[])
{
  MDB_env * handle;
  struct emdb_map_t * node;
  unsigned long addr;
  char * err;
  int ret;
  
  if (! enif_get_ulong(env, argv[0], & addr))
    return enif_make_badarg(env);
  
  handle = (MDB_env *) addr;
  
  HASH_FIND_PTR(emdb_map, & handle, node);
  if (NULL == node)
    FAIL_FAST(EMDB_INVALID_HANDLE_ERR, err1);

  if (node -> cursor != NULL)
    FAIL_FAST(EMDB_CURSOR_IN_USE_ERR, err1);

  if (node -> txn == NULL)
    if (mdb_txn_begin(handle, NULL, 0, & (node -> txn)))
      FAIL_FAST(EMDB_TXN_BEGIN_ERR, err1);
  
  if( mdb_cursor_open(node -> txn, node -> dbi, &(node -> cursor)) )
    FAIL_FAST(EMDB_CURSOR_OPEN_ERR, err1);
  
  return atom_ok;
  
 err1:
 
  return enif_make_tuple(env, 2, 
			 atom_error,
			 enif_make_atom(env, err));
  
}




static ERL_NIF_TERM emdb_cursor_close_nif (ErlNifEnv * env,
					   int argc, const ERL_NIF_TERM argv[])
{
  MDB_env * handle;
  struct emdb_map_t * node;
  unsigned long addr;
  char * err;
  int ret;
  
  printf("start of cursor_close_nif\n");
    
  if (! enif_get_ulong(env, argv[0], & addr))
    return enif_make_badarg(env);
  
  handle = (MDB_env *) addr;
  
  HASH_FIND_PTR(emdb_map, & handle, node);
  if (NULL == node)
    FAIL_FAST(EMDB_INVALID_HANDLE_ERR, err1);

  if (node -> cursor == NULL)
    FAIL_FAST(EMDB_CURSOR_NO_CURSOR_ERR, err1);
  
  printf("next step: closing the cursor\n");
  
  mdb_cursor_close(node -> cursor);
    
  if (mdb_txn_commit(node -> txn))
    FAIL_FAST(EMDB_TXN_COMMIT_ERR, err1);
  
  node -> cursor = NULL;
  node -> txn = NULL;

  return atom_ok;
  
 err1:

  return enif_make_tuple(env, 2, 
			 atom_error,
			 enif_make_atom(env, err));

}




static ERL_NIF_TERM emdb_cursor_next_nif (ErlNifEnv * env,
					   int argc, const ERL_NIF_TERM argv[])
{
  MDB_env * handle;
  struct emdb_map_t * node;
  unsigned long addr;
  char * err;

  MDB_val rkey;
  MDB_val rdata;

  ERL_NIF_TERM kterm;  
  ERL_NIF_TERM vterm;

  ErlNifBinary key = {0};
  ErlNifBinary val = {0};
  
  if (! enif_get_ulong(env, argv[0], & addr))
    return enif_make_badarg(env);
  
  handle = (MDB_env *) addr;
  
  HASH_FIND_PTR(emdb_map, & handle, node);
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
  
  vterm = enif_make_binary(env, &val);
  kterm = enif_make_binary(env, &key);

  if (! vterm || ! kterm)
    FAIL_FAST(EMDB_MAKE_BINARY_ERR, err1);

  return enif_make_tuple(env, 2,
                         atom_ok,
                         enif_make_tuple(env, 2, kterm, vterm));
  
 err1:

  return enif_make_tuple(env, 2, 
			 atom_error,
			 enif_make_atom(env, err));

}



static ERL_NIF_TERM emdb_cursor_set_nif (ErlNifEnv * env,
					   int argc, const ERL_NIF_TERM argv[])
{
  MDB_env * handle;
  struct emdb_map_t * node;
  unsigned long addr;
  char * err;

  MDB_val rkey;
  MDB_val rdata;

  ERL_NIF_TERM kterm;  
  ERL_NIF_TERM vterm;

  ErlNifBinary key = {0};
  ErlNifBinary val = {0};
  
  if (! enif_get_ulong(env, argv[0], & addr))
    return enif_make_badarg(env);
  
  handle = (MDB_env *) addr;
  
  HASH_FIND_PTR(emdb_map, & handle, node);
  if (NULL == node)
    FAIL_FAST(EMDB_INVALID_HANDLE_ERR, err1);
  
  if (node -> cursor == NULL)
    FAIL_FAST(EMDB_CURSOR_NO_CURSOR_ERR, err1);
  
  if (! enif_inspect_iolist_as_binary(env, argv[1], &key))
    return enif_make_badarg(env);

  rkey.mv_size = key.size;
  rkey.mv_data = key.data;

  if ( mdb_cursor_get(node -> cursor, &rkey, &rdata, MDB_SET_KEY) ) 
    FAIL_FAST(EMDB_CURSOR_SET_ERR, err1);

  val.size = rdata.mv_size;
  val.data = rdata.mv_data;

  key.size = rkey.mv_size;
  key.data = rkey.mv_data;
  
  vterm = enif_make_binary(env, &val);
  kterm = enif_make_binary(env, &key);

  if (! vterm || ! kterm)
    FAIL_FAST(EMDB_MAKE_BINARY_ERR, err1);

  return enif_make_tuple(env, 2,
                         atom_ok,
                         enif_make_tuple(env, 2, kterm, vterm));
  
 err1:

  return enif_make_tuple(env, 2, 
			 atom_error,
			 enif_make_atom(env, err));
  
}

static ERL_NIF_TERM emdb_cursor_del_nif (ErlNifEnv * env,
					   int argc, const ERL_NIF_TERM argv[])
{
  MDB_env * handle;
  struct emdb_map_t * node;
  unsigned long addr;
  char * err;
  
  ERL_NIF_TERM kterm;  
  ERL_NIF_TERM vterm;

  ErlNifBinary key = {0};
  ErlNifBinary val = {0};


  MDB_val rkey;
  MDB_val rdata;
  
  if (! enif_get_ulong(env, argv[0], & addr))
    return enif_make_badarg(env);
  
  handle = (MDB_env *) addr;
  
  HASH_FIND_PTR(emdb_map, & handle, node);
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
  
  vterm = enif_make_binary(env, &val);
  kterm = enif_make_binary(env, &key);

  if (! vterm || ! kterm)
    FAIL_FAST(EMDB_MAKE_BINARY_ERR, err1);

  return enif_make_tuple(env, 2,
                         atom_ok,
                         enif_make_tuple(env, 2, kterm, vterm));

 err1:
  return enif_make_tuple(env, 2, 
			 atom_error,
			 enif_make_atom(env, err));
}


static int emdb_load(ErlNifEnv* env, void** priv, ERL_NIF_TERM load_info)
  {
    atom_ok    = enif_make_atom(env, "ok");
    atom_undefined  = enif_make_atom(env, "undefined");
    atom_error = enif_make_atom(env, "error");

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
  {"open",        3, emdb_open_nif},
  {"close",       1, emdb_close_nif},
  {"put",         3, emdb_put_nif},
  {"append",      3, emdb_append_nif},
  {"get",         2, emdb_get_nif},
  {"del",         2, emdb_del_nif},
  {"update",      3, emdb_update_nif},
  {"drop",        1, emdb_drop_nif},
  {"cursor_open", 1, emdb_cursor_open_nif},
  {"cursor_close",1, emdb_cursor_close_nif},
  {"cursor_next", 1, emdb_cursor_next_nif},
  {"cursor_set",  2, emdb_cursor_set_nif},
  {"cursor_del",  1, emdb_cursor_del_nif},
  {"txn_begin",   1, emdb_txn_begin_nif},
  {"txn_begin_ro",1, emdb_txn_begin_ro_nif},
  {"txn_commit",  1, emdb_txn_commit_nif}
};

/* driver entry point */
ERL_NIF_INIT(emdb_drv,
             nif_funcs,
             & emdb_load,
             & emdb_reload,
             & emdb_upgrade,
             & emdb_unload)
