#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <git2.h>
#include <git2/refdb.h>
#include <git2/refs.h>

int git_odb_backend_mysql_open(git_odb_backend **odb_backend_out,
         git_refdb_backend **refdb_backend_out,
         const char *mysql_host,
         const char *mysql_user, const char *mysql_passwd, const char *mysql_db,
         unsigned int mysql_port, const char *mysql_unix_socket,
         unsigned long mysql_client_flag);

int git_odb_backend_mysql_create(const char *mysql_host,
         const char *mysql_user, const char *mysql_passwd, const char *mysql_db,
         unsigned int mysql_port, const char *mysql_unix_socket,
         unsigned long mysql_client_flag);

void
nope(const char *message, ...)
{
  va_list args;
  va_start(args, message);

  vfprintf(stderr, message, args);
  abort();
}

void
init_git()
{
  git_threads_init();
}

static void
getenv_data(const char **hostname, const char **username, const char **password,
        const char **dbname, const char **portno, const char **unixsocket)
{

  *hostname = getenv("LIBGIT2_MYSQL_TEST_HOSTNAME");
  if (!*hostname)
    nope("Expected test mysql server hostname in environment\n");

  *username = getenv("LIBGIT2_MYSQL_TEST_USERNAME");
  if (!*username)
    nope("Expected test mysql server username in environment\n");

  *password = getenv("LIBGIT2_MYSQL_TEST_PASSWORD");
  if (!*username)
    nope("Expected test mysql server password in environment\n");

  *dbname = getenv("LIBGIT2_MYSQL_TEST_DBNAME");
  if (!*username)
    nope("Expected test mysql server database name in environment\n");

  *portno = getenv("LIBGIT2_MYSQL_TEST_PORTNO");
  *unixsocket = getenv("LIBGIT2_MYSQL_TEST_UNIXSOCKET");
}

int
create_repo_from_env()
{
  const char *hostname, *username, *password, *dbname, *portno, *unixsocket;
  unsigned int actual_portno;
  int error;

  getenv_data(&hostname, &username, &password, &dbname, &portno, &unixsocket);

  if (portno)
    actual_portno = atoi(portno);
  else
    actual_portno = 3306;

  error = git_odb_backend_mysql_create(hostname, username, password, dbname,
                actual_portno, unixsocket, 0);
  return error;
}

git_repository *
open_repo_from_env()
{
  git_odb_backend *odb_out;
  git_refdb_backend *refdb_out;
  git_repository *repository;
  git_odb *odb;
  git_refdb *refdb;
  const char *hostname, *username, *password, *dbname, *portno, *unixsocket;
  unsigned int actual_portno;
  int error;

  getenv_data(&hostname, &username, &password, &dbname, &portno, &unixsocket);

  if (portno)
    actual_portno = atoi(portno);
  else
    actual_portno = 3306;

  odb_out = NULL;
  refdb_out = NULL;
  error = git_odb_backend_mysql_open(&odb_out, &refdb_out, hostname, username,
		  password, dbname, actual_portno, unixsocket, 0);

  if (error != GIT_OK)
    nope("Failed to open mysql git repo, error status %d\n", error);

  if (!odb_out || !refdb_out)
    nope("Backend open call did not set either odb_out or refdb_out\n");

  /* We have successfully created a custom backend. Now, create an odb around
   * it, and then wrap it in a repository. */
  error = git_odb_new(&odb);
  if (error != GIT_OK)
    nope("Failed to create new odb obj, error %d\n", error);

  error = git_odb_add_backend(odb, odb_out, 0);
  if (error != GIT_OK)
    nope("Failed to insert backed into odb obj, error %d\n", error);

  error = git_repository_wrap_odb(&repository, odb);
  if (error != GIT_OK)
    nope("Failed to create git repo from odb, error %d\n", error);

  /* Create a new reference database obj, add our custom backend, shoehorn into
   * repository */
  error = git_refdb_new(&refdb, repository);
  if (error != GIT_OK)
    nope("Failed to create new refdb with repo, error %d\n", error);

  error = git_refdb_set_backend(refdb, refdb_out);
  if (error != GIT_OK)
    nope("Failed to set refdb backend, error %d\n", error);

  /* Can't fail */
  git_repository_set_refdb(repository, refdb);

  /* Decrease reference count on both refdb and odb backends -- they'll be
   * kept alive, but only by one reference, held by the repository */
  git_refdb_free(refdb);
  git_odb_free(odb);

  return repository; 
}
