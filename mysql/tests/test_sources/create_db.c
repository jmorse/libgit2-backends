#include "../common.h"

int
main()
{
	int error;
       
	error = create_repo_from_env();
	if (error != GIT_OK)
		nope("Create repo function returned %d\n", error);

	return 0;
}
