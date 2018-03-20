# ssr_backup

This probe/script has been created to make a save of SSR/MCS Profiles with the help of selservice-cli.jar to import and export profile.

Originally created to work with multi-threading (it doesn't work because of a bug of the JAVA executable).

## Requirement

- Install ssrbackup_ref table with the create_table.sql file
- selfservice-cli.jar (at the root of the script).

## Roadmap  

- Export manually (device and group) with a callback.

## Know defects 

- selfservice-cli.jar timeout arg 
- Import Log monitoring for device return an error.
- selfservice-cli.jar not able to scale

Failed to import profile. Error: Failed to create profile. Error: Failed to make POST request for URL [http://10.253.30.27/ssrws/devices/3908/profiles]. Status code was [400]. 
Output was : [com.nimsoft.selfservice.exceptions.InvalidProfileException: The profile does not exist]