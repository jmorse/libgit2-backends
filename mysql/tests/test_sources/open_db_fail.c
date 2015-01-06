#include "../common.h"

int
main()
{
	int error;
	git_repository *repo;

	/* This should fail if the tables were not created */
	repo = open_repo_from_env();
	if (!repo)
		nope("Could not open git repo");

	return 0;
}
