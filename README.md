QFS
===

```bash
virtualenv -p python3 ./venv
source venv/bin/activate
pip install -r requirements.txt
MOUNT=$HOME/qfs python fs.py example.json
```