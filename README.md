### Artifactory Replicator
A lightweight replicator for large Artifactory (Pro|Ent) repositories. The
script utilises two Artifactory API endpoints: "File list" to obtain
source/destination artifact listings and "Artifact sync download" to pre-cache
artifacts to the destination service so achieving replication
(https://www.jfrog.com/confluence/display/RTF/Artifactory+REST+API, 2019).

This script serves to circumvent limitations in Artifactory's built-in
replication mechanisms that prevents expedient replication of single large
repositories. This approach has been validated against repositories containing
in excess of 600000 artifacts (virtual size around 13 TB, actual around 4 TB).

## Installation and usage:
1. Clone the project:
```
git clone git@github.com:alexandertgtalbot/artifactory_replicator.git
cd ./artifactory_replicator
```

2. Copy the replicator script to the desired location:
```
cp ./artifactory_replicator.py /some_directory/
chown some_user:some_user /some_directory/artifactory_replicator.py
chmod 0755 /some_directory/artifactory_replicator.py
```

3. Copy the example config to the required location (in this example the home
   directory of the user (some_user) from the example above:
```
mkdir /home/some_user/.artifactory_replicator
cp ./example_config.json /home/some_user/.artifactory_replicator/config.json
chown some_user:some_user /home/some_user/.artifactory_replicator/config.json
chmod 0600 /home/some_user/.artifactory_replicator/config.json
```
4. Edit '/home/some_user/.artifactory_replicator/config.json' setting
   appropriate values.

5. Run the script (preferably in a screen session) as follows:
```
/some_directory/artifactory_replicator.py

# OR

screen /some_directory/artifactory_replicator.py
```
