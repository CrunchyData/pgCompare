# pgCompare Container

## Build Container

For building instructions, see the comments in the `Dockerfile`.

## Using Container

Create a `pgcompare.properties` file as outlined in the project README file.  The properties file will be mounted in the /etc/pgcompare directory of the container as seen below.  It can be placed in an alternate location by setting the PGCOMPARE_CONFIG environment variable.

```shell
docker run --name pgcompare \
           -v /Users/bpace/app/gitecto/db-projects/pgCompare-Test/pgcompare.properties:/etc/pgcompare/pgcompare.properties \
           -e PGCOMPARE_OPTIONS="--batch 0 --project 1" \
           cbrianpace/pgcompare:v0.3.1

podman run --name pgcompare \
           -v /Users/bpace/app/gitecto/db-projects/pgCompare-Test/pgcompare.properties:/etc/pgcompare/pgcompare.properties \
           -e PGCOMPARE_OPTIONS="--batch 0 --project 1" \
           cbrianpace/pgcompare:v0.3.1

```

