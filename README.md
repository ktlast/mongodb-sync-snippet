# mongodb-sync-snippet
Sync MongoDB data between 2 Clusters

<br>

### start

```bash
chmod a+x ./main.py
python3 ./main.py
```

<br>


### Note
- This script simply loops every database (except: `local`, `config`, `admin`) and every collection, insert all docs into new cluster.
