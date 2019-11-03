# echo $(PULUMI_ENV_VARS) | tr " " "\n" > cloud/test/.env

PULUMI_ENV_VARS := $(shell cd cloud/pulumi && pulumi stack output | grep _ | awk '{print $$1 "=" $$2}')

pulumi_stack_output-to-env: # Maps the Stack output from Pulumi to a .env in the specified directory
	cd ${STACK_PATH} \
	&& pulumi stack select ${STACK} \
	&& echo $(pulumi stack output | grep _ | awk '{print $$1 "=" $$2}' | tr " " "\n") > test.env
