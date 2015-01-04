/*
 * This file is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2,
 * as published by the Free Software Foundation.
 *
 * In addition to the permissions in the GNU General Public License,
 * the authors give you unlimited permission to link the compiled
 * version of this file into combinations with other programs,
 * and to distribute those combinations without any restriction
 * coming from the use of this file.  (The General Public License
 * restrictions do apply in other respects; for example, they cover
 * modification of the file, and distribution when not linked into
 * a combined executable.)
 *
 * This file is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; see the file COPYING.  If not, write to
 * the Free Software Foundation, 51 Franklin Street, Fifth Floor,
 * Boston, MA 02110-1301, USA.
 */

#include <assert.h>
#include <string.h>
#include <git2.h>
#include <git2/odb_backend.h>
#include <git2/sys/odb_backend.h>
#include <git2/errors.h>
#include <git2/types.h>

/* MySQL C Api docs:
 *   http://dev.mysql.com/doc/refman/5.1/en/c-api-function-overview.html
 *
 * And the prepared statement API docs:
 *   http://dev.mysql.com/doc/refman/5.1/en/c-api-prepared-statement-function-overview.html
 */
#include <mysql.h>

#define GIT2_ODB_TABLE_NAME "git2_odb"
#define GIT2_ODB_STORAGE_ENGINE "InnoDB"

typedef struct {
  git_odb_backend parent;
  MYSQL *db;
  MYSQL_STMT *st_read;
  MYSQL_STMT *st_write;
  MYSQL_STMT *st_read_header;
  MYSQL_STMT *st_read_prefix;
} mysql_odb_backend;

static int mysql_odb_backend__read_header(size_t *len_p, git_otype *type_p, git_odb_backend *_backend, const git_oid *oid)
{
  mysql_odb_backend *backend;
  int error;
  MYSQL_BIND bind_buffers[1];
  MYSQL_BIND result_buffers[2];

  assert(len_p && type_p && _backend && oid);

  backend = (mysql_odb_backend *)_backend;
  error = GIT_ERROR;

  memset(bind_buffers, 0, sizeof(bind_buffers));
  memset(result_buffers, 0, sizeof(result_buffers));

  // bind the oid passed to the statement
  bind_buffers[0].buffer = (void*)oid->id;
  bind_buffers[0].buffer_length = 20;
  bind_buffers[0].length = &bind_buffers[0].buffer_length;
  bind_buffers[0].buffer_type = MYSQL_TYPE_BLOB;
  if (mysql_stmt_bind_param(backend->st_read_header, bind_buffers) != 0)
    return 0;

  // execute the statement
  if (mysql_stmt_execute(backend->st_read_header) != 0)
    return 0;

  if (mysql_stmt_store_result(backend->st_read_header) != 0)
    return 0;

  // this should either be 0 or 1
  // if it's > 1 MySQL's unique index failed and we should all fear for our lives
  if (mysql_stmt_num_rows(backend->st_read_header) == 1) {
    result_buffers[0].buffer_type = MYSQL_TYPE_TINY;
    result_buffers[0].buffer = type_p;
    result_buffers[0].buffer_length = sizeof(type_p);
    memset(type_p, 0, sizeof(*type_p));

    result_buffers[1].buffer_type = MYSQL_TYPE_LONGLONG;
    result_buffers[1].buffer = len_p;
    result_buffers[1].buffer_length = sizeof(len_p);
    memset(len_p, 0, sizeof(*len_p));

    if(mysql_stmt_bind_result(backend->st_read_header, result_buffers) != 0)
      return GIT_ERROR;

    // this should populate the buffers at *type_p and *len_p
    if(mysql_stmt_fetch(backend->st_read_header) != 0)
      return GIT_ERROR;

    error = GIT_OK;
  } else {
    error = GIT_ENOTFOUND;
  }

  // reset the statement for further use
  if (mysql_stmt_reset(backend->st_read_header) != 0)
    return 0;

  return error;
}

static int mysql_odb_backend__read_prefix(git_oid *output_oid, void **out_buf,
        size_t *out_len, git_otype *out_type, git_odb_backend *_backend,
        const git_oid *partial_oid, size_t oidlen)
{
  MYSQL_BIND result_buffers[3];
  MYSQL_BIND bind_buffers[1];
  mysql_odb_backend *backend;
  unsigned long data_len;
  int error = GIT_ERROR;

  assert(output_oid && out_buf && out_len && out_type && backend && partial_oid
          && oidlen != 0);

  backend = (mysql_odb_backend *)_backend;

  memset(result_buffers, 0, sizeof(result_buffers));
  memset(bind_buffers, 0, sizeof(bind_buffers));

  // Incoming OID length is specified in _hex_ digits, not bytes. Decrease to
  // bytes for mysql. XXX this shows up one problem, which is that we can't
  // search prefixes by anything less than a byte, and extra nibbles will be
  // truncated. This is not easily fixed.
  oidlen /= 2;

  // Bind the partial oid into the statement
  bind_buffers[0].buffer = (void*)partial_oid->id;
  bind_buffers[0].buffer_length = oidlen;
  bind_buffers[0].length = &bind_buffers[0].buffer_length;
  bind_buffers[0].buffer_type = MYSQL_TYPE_BLOB;
  if (mysql_stmt_bind_param(backend->st_read_prefix, bind_buffers) != 0)
    return error;

  // execute the statement
  if (mysql_stmt_execute(backend->st_read_prefix) != 0)
    return error;

  if (mysql_stmt_store_result(backend->st_read_prefix) != 0)
    return error;

  // This could be 0, 1, or many: it's a prefix search.
  if (mysql_stmt_num_rows(backend->st_read_prefix) == 0) {
    error = GIT_ENOTFOUND;
  } else if (mysql_stmt_num_rows(backend->st_read_prefix) > 1) {
    error = GIT_EAMBIGUOUS;
  } else {
    assert(mysql_stmt_num_rows(backend->st_read_prefix) == 1);

    result_buffers[0].buffer_type = MYSQL_TYPE_TINY;
    result_buffers[0].buffer = &out_type;
    result_buffers[0].buffer_length = sizeof(*out_type);
    memset(out_type, 0, sizeof(*out_type));

    result_buffers[1].buffer_type = MYSQL_TYPE_LONGLONG;
    result_buffers[1].buffer = out_len;
    result_buffers[1].buffer_length = sizeof(out_len);
    memset(out_len, 0, sizeof(*out_len));

    result_buffers[2].buffer_type = MYSQL_TYPE_LONG_BLOB;
    result_buffers[2].buffer = 0;
    result_buffers[2].buffer_length = 0;
    result_buffers[2].length = &data_len;

    if(mysql_stmt_bind_result(backend->st_read_prefix, result_buffers) != 0)
      return GIT_ERROR;

    // Fetch row, binding output data values, except data column
    error = mysql_stmt_fetch(backend->st_read_prefix);

    if (data_len > 0) {
      *out_buf = malloc(data_len);
      result_buffers[2].buffer = *out_buf;
      result_buffers[2].buffer_length = data_len;

      if (mysql_stmt_fetch_column(backend->st_read_prefix, &result_buffers[2], 2, 0) != 0)
        return GIT_ERROR;
    }

    error = GIT_OK;
  }

  // reset the statement for further use
  if (mysql_stmt_reset(backend->st_read_prefix) != 0)
    return GIT_ERROR;

  return error;
}

static int mysql_odb_backend__read(void **data_p, size_t *len_p, git_otype *type_p, git_odb_backend *_backend, const git_oid *oid)
{
  mysql_odb_backend *backend;
  int error;
  MYSQL_BIND bind_buffers[1];
  MYSQL_BIND result_buffers[3];
  unsigned long data_len;

  assert(len_p && type_p && _backend && oid);

  backend = (mysql_odb_backend *)_backend;
  error = GIT_ERROR;

  memset(bind_buffers, 0, sizeof(bind_buffers));
  memset(result_buffers, 0, sizeof(result_buffers));

  // bind the oid passed to the statement
  bind_buffers[0].buffer = (void*)oid->id;
  bind_buffers[0].buffer_length = 20;
  bind_buffers[0].length = &bind_buffers[0].buffer_length;
  bind_buffers[0].buffer_type = MYSQL_TYPE_BLOB;
  if (mysql_stmt_bind_param(backend->st_read, bind_buffers) != 0)
    return 0;

  // execute the statement
  if (mysql_stmt_execute(backend->st_read) != 0)
    return 0;

  if (mysql_stmt_store_result(backend->st_read) != 0)
    return 0;

  // this should either be 0 or 1
  // if it's > 1 MySQL's unique index failed and we should all fear for our lives
  if (mysql_stmt_num_rows(backend->st_read) == 1) {
    result_buffers[0].buffer_type = MYSQL_TYPE_TINY;
    result_buffers[0].buffer = type_p;
    result_buffers[0].buffer_length = sizeof(type_p);
    memset(type_p, 0, sizeof(*type_p));

    result_buffers[1].buffer_type = MYSQL_TYPE_LONGLONG;
    result_buffers[1].buffer = len_p;
    result_buffers[1].buffer_length = sizeof(len_p);
    memset(len_p, 0, sizeof(*len_p));

    // by setting buffer and buffer_length to 0, this tells libmysql
    // we want it to set data_len to the *actual* length of that field
    // this way we can malloc exactly as much memory as we need for the buffer
    //
    // come to think of it, we can probably just use the length set in *len_p
    // once we fetch the result?
    result_buffers[2].buffer_type = MYSQL_TYPE_LONG_BLOB;
    result_buffers[2].buffer = 0;
    result_buffers[2].buffer_length = 0;
    result_buffers[2].length = &data_len;

    if(mysql_stmt_bind_result(backend->st_read, result_buffers) != 0)
      return GIT_ERROR;

    // this should populate the buffers at *type_p, *len_p and &data_len
    error = mysql_stmt_fetch(backend->st_read);
    // if(error != 0 || error != MYSQL_DATA_TRUNCATED)
    //   return GIT_ERROR;

    if (data_len > 0) {
      *data_p = malloc(data_len);
      result_buffers[2].buffer = *data_p;
      result_buffers[2].buffer_length = data_len;

      if (mysql_stmt_fetch_column(backend->st_read, &result_buffers[2], 2, 0) != 0)
        return GIT_ERROR;
    }

    error = GIT_OK;
  } else {
    error = GIT_ENOTFOUND;
  }

  // reset the statement for further use
  if (mysql_stmt_reset(backend->st_read) != 0)
    return 0;

  return error;
}

static int mysql_odb_backend__exists(git_odb_backend *_backend, const git_oid *oid)
{
  mysql_odb_backend *backend;
  int found;
  MYSQL_BIND bind_buffers[1];

  assert(_backend && oid);

  backend = (mysql_odb_backend *)_backend;
  found = 0;

  memset(bind_buffers, 0, sizeof(bind_buffers));

  // bind the oid passed to the statement
  bind_buffers[0].buffer = (void*)oid->id;
  bind_buffers[0].buffer_length = 20;
  bind_buffers[0].length = &bind_buffers[0].buffer_length;
  bind_buffers[0].buffer_type = MYSQL_TYPE_BLOB;
  if (mysql_stmt_bind_param(backend->st_read_header, bind_buffers) != 0)
    return 0;

  // execute the statement
  if (mysql_stmt_execute(backend->st_read_header) != 0)
    return 0;

  if (mysql_stmt_store_result(backend->st_read_header) != 0)
    return 0;

  // now lets see if any rows matched our query
  // this should either be 0 or 1
  // if it's > 1 MySQL's unique index failed and we should all fear for our lives
  if (mysql_stmt_num_rows(backend->st_read_header) == 1) {
    found = 1;
  }

  // reset the statement for further use
  if (mysql_stmt_reset(backend->st_read_header) != 0)
    return 0;

  return found;
}

static int mysql_odb_backend__write(git_oid *oid, git_odb_backend *_backend, const void *data, size_t len, git_otype type)
{
  int error;
  mysql_odb_backend *backend;
  MYSQL_BIND bind_buffers[4];
  my_ulonglong affected_rows;

  assert(oid && _backend && data);

  backend = (mysql_odb_backend *)_backend;

  if ((error = git_odb_hash(oid, data, len, type)) < 0)
    return error;

  memset(bind_buffers, 0, sizeof(bind_buffers));

  // bind the oid
  bind_buffers[0].buffer = (void*)oid->id;
  bind_buffers[0].buffer_length = 20;
  bind_buffers[0].length = &bind_buffers[0].buffer_length;
  bind_buffers[0].buffer_type = MYSQL_TYPE_BLOB;

  // bind the type
  bind_buffers[1].buffer = &type;
  bind_buffers[1].buffer_type = MYSQL_TYPE_TINY;

  // bind the size of the data
  bind_buffers[2].buffer = &len;
  bind_buffers[2].buffer_type = MYSQL_TYPE_LONG;

  // bind the data
  bind_buffers[3].buffer = (void*)data;
  bind_buffers[3].buffer_length = len;
  bind_buffers[3].length = &bind_buffers[3].buffer_length;
  bind_buffers[3].buffer_type = MYSQL_TYPE_BLOB;

  if (mysql_stmt_bind_param(backend->st_write, bind_buffers) != 0)
    return GIT_ERROR;

  // TODO: use the streaming backend API so this actually makes sense to use :P
  // once we want to use this we should comment out 
  // if (mysql_stmt_send_long_data(backend->st_write, 2, data, len) != 0)
  //   return GIT_ERROR;

  // execute the statement
  if (mysql_stmt_execute(backend->st_write) != 0)
    return GIT_ERROR;

  // now lets see if the insert worked
  affected_rows = mysql_stmt_affected_rows(backend->st_write);
  if (affected_rows != 1)
    return GIT_ERROR;

  // reset the statement for further use
  if (mysql_stmt_reset(backend->st_read_header) != 0)
    return GIT_ERROR;

  return GIT_OK;
}

static void mysql_odb_backend__free(git_odb_backend *_backend)
{
  mysql_odb_backend *backend;
  assert(_backend);
  backend = (mysql_odb_backend *)_backend;

  if (backend->st_read)
    mysql_stmt_close(backend->st_read);
  if (backend->st_read_header)
    mysql_stmt_close(backend->st_read_header);
  if (backend->st_write)
    mysql_stmt_close(backend->st_write);
  if (backend->st_read_prefix)
    mysql_stmt_close(backend->st_read_prefix);

  mysql_close(backend->db);

  free(backend);
}

static int create_table(MYSQL *db)
{
  static const char *sql_create =
    "CREATE TABLE `" GIT2_ODB_TABLE_NAME "` ("
    "  `oid` binary(20) NOT NULL DEFAULT '',"
    "  `type` tinyint(1) unsigned NOT NULL,"
    "  `size` bigint(20) unsigned NOT NULL,"
    "  `data` longblob NOT NULL,"
    "  PRIMARY KEY (`oid`),"
    "  KEY `type` (`type`),"
    "  KEY `size` (`size`)"
    ") ENGINE=" GIT2_ODB_STORAGE_ENGINE " DEFAULT CHARSET=utf8 COLLATE=utf8_bin;";

  if (mysql_real_query(db, sql_create, strlen(sql_create)) != 0)
    return GIT_ERROR;

  return GIT_OK;
}

static int init_db(MYSQL *db)
{
  static const char *sql_check =
    "SHOW TABLES LIKE '" GIT2_ODB_TABLE_NAME "';";

  MYSQL_RES *res;
  int error;
  my_ulonglong num_rows;

  if (mysql_real_query(db, sql_check, strlen(sql_check)) != 0)
    return GIT_ERROR;

  res = mysql_store_result(db);
  if (res == NULL)
    return GIT_ERROR;

  num_rows = mysql_num_rows(res);
  if (num_rows == 0) {
    /* the table was not found */
    error = GIT_ENOTFOUND;
  } else if (num_rows > 0) {
    /* the table was found */
    error = GIT_OK;
  } else {
    error = GIT_ERROR;
  }

  mysql_free_result(res);
  return error;
}

static int init_statements(mysql_odb_backend *backend)
{
  my_bool truth = 1;

  static const char *sql_read =
    "SELECT `type`, `size`, UNCOMPRESS(`data`) FROM `" GIT2_ODB_TABLE_NAME "` WHERE `oid` = ?;";

  static const char *sql_read_header =
    "SELECT `type`, `size` FROM `" GIT2_ODB_TABLE_NAME "` WHERE `oid` = ?;";

  static const char *sql_read_prefix =
    "SELECT `type`, `size`, UNCOMPRESS(`data`) FROM `" GIT2_ODB_TABLE_NAME "` WHERE oid LIKE CONCAT(?, '%');";

  static const char *sql_write =
    "INSERT IGNORE INTO `" GIT2_ODB_TABLE_NAME "` VALUES (?, ?, ?, COMPRESS(?));";

  backend->st_read = mysql_stmt_init(backend->db);
  if (backend->st_read == NULL)
    return GIT_ERROR;

  if (mysql_stmt_attr_set(backend->st_read, STMT_ATTR_UPDATE_MAX_LENGTH, &truth) != 0)
    return GIT_ERROR;

  if (mysql_stmt_prepare(backend->st_read, sql_read, strlen(sql_read)) != 0)
    return GIT_ERROR;


  backend->st_read_header = mysql_stmt_init(backend->db);
  if (backend->st_read_header == NULL)
    return GIT_ERROR;

  if (mysql_stmt_attr_set(backend->st_read_header, STMT_ATTR_UPDATE_MAX_LENGTH, &truth) != 0)
    return GIT_ERROR;

  if (mysql_stmt_prepare(backend->st_read_header, sql_read_header, strlen(sql_read)) != 0)
    return GIT_ERROR;


  backend->st_read_prefix = mysql_stmt_init(backend->db);
  if (backend->st_read_prefix == NULL)
    return GIT_ERROR;

  if (mysql_stmt_attr_set(backend->st_read_prefix, STMT_ATTR_UPDATE_MAX_LENGTH, &truth) != 0)
    return GIT_ERROR;

  if (mysql_stmt_prepare(backend->st_read_prefix, sql_read_prefix, strlen(sql_read_prefix)) != 0)
    return GIT_ERROR;


  backend->st_write = mysql_stmt_init(backend->db);
  if (backend->st_write == NULL)
    return GIT_ERROR;

  if (mysql_stmt_attr_set(backend->st_write, STMT_ATTR_UPDATE_MAX_LENGTH, &truth) != 0)
    return GIT_ERROR;

  if (mysql_stmt_prepare(backend->st_write, sql_write, strlen(sql_read)) != 0)
    return GIT_ERROR;


  return GIT_OK;
}

static MYSQL *connect_to_server(const char *mysql_host, const char *mysql_user,
        const char *mysql_passwd, const char *mysql_db, unsigned int mysql_port,
        const char *mysql_unix_socket, unsigned long mysql_client_flag)
{
  my_bool reconnect;

  MYSQL *db = mysql_init(NULL);

  reconnect = 1;
  // allow libmysql to reconnect gracefully
  if (mysql_options(db, MYSQL_OPT_RECONNECT, &reconnect) != 0)
    goto cleanup;

  // make the connection
  if (mysql_real_connect(db, mysql_host, mysql_user, mysql_passwd, mysql_db, mysql_port, mysql_unix_socket, mysql_client_flag) != db)
    goto cleanup;

  return db;

cleanup:
  mysql_close(db);
  return NULL;
}

int git_odb_backend_mysql_open(git_odb_backend **backend_out, const char *mysql_host,
        const char *mysql_user, const char *mysql_passwd, const char *mysql_db,
        unsigned int mysql_port, const char *mysql_unix_socket, unsigned long mysql_client_flag)
{
  mysql_odb_backend *backend;
  int error = GIT_ERROR;

  backend = calloc(1, sizeof(mysql_odb_backend));
  if (backend == NULL) {
    giterr_set_oom();
    return GIT_ERROR;
  }

  backend->db = connect_to_server(mysql_host, mysql_user, mysql_passwd,
                       mysql_db, mysql_port, mysql_unix_socket, mysql_client_flag);

  if (!backend->db)
    goto cleanup;

  // check for existence of db
  error = init_db(backend->db);
  if (error < 0)
    goto cleanup;

  error = init_statements(backend);
  if (error < 0)
    goto cleanup;

  backend->parent.version = GIT_ODB_BACKEND_VERSION;
  backend->parent.odb = NULL;
  backend->parent.read = &mysql_odb_backend__read;
  backend->parent.read_header = &mysql_odb_backend__read_header;
  backend->parent.read_prefix = &mysql_odb_backend__read_prefix;
  backend->parent.write = &mysql_odb_backend__write;
  backend->parent.exists = &mysql_odb_backend__exists;
  backend->parent.free = &mysql_odb_backend__free;

  *backend_out = (git_odb_backend *)backend;
  return GIT_OK;

cleanup:
  mysql_odb_backend__free((git_odb_backend *)backend);
  return error;
}

int git_odb_backend_mysql_create(const char *mysql_host, const char *mysql_user,
        const char *mysql_passwd, const char *mysql_db, unsigned int mysql_port,
        const char *mysql_unix_socket, unsigned long mysql_client_flag)
{
  MYSQL *db;
  int error = GIT_ERROR;

  db = connect_to_server(mysql_host, mysql_user, mysql_passwd,
               mysql_db, mysql_port, mysql_unix_socket, mysql_client_flag);

  if (!db)
    goto cleanup;

  error = create_table(db);

cleanup:
  if (db)
    mysql_close(db);

  return error;
}

void git_odb_backend_mysql_free(git_odb_backend *backend)
{
  /* Function for disposing of an unwanted backend -- necessary if for some
   * reason it can't be loaded into a git_repository. There's no other
   * publically published way to get rid of it. */
  mysql_odb_backend__free(backend);
  return;
}
