#include <git2.h>

git_repository *open_repo_from_env();
int create_repo_from_env();
void nope(const char *format, ...);
