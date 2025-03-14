# flanker
Openside flankers need to be quick, agile, and possess exceptional ball-hunting skills.

## For what I use it?

Whenever I want to upload files, eg. from a k8s POD. Where files are larger 5GB.
Initially I was using pre-signed AWS URLs, but it seems to be a bit hacky to put
them together and create various files. That is nothing I want todo execing into a POD.

The script is ready to use `uv`, make sure you have the env variables set to allow AUTH:

```shell
uv run https://raw.githubusercontent.com/WalBeh/flanker/refs/heads/main/flanker.py --key ... --bucket
```
