#include "../common.h"

int
main()
{
	int error;
	git_repository *repo;

	create_repo_from_env();
	repo = open_repo_from_env();
	if (!repo)
		nope("Could not open git repo");

	return 0;
}
