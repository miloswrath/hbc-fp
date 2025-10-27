import requests

# Request xpt file from URL
url = "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2011/DataFiles/DEMO_G.xpt"

r = requests.get(url)

open("datafile.xpt", "wb").write(r.content)

import pandas as pd, pyreadstat

df = pyreadstat.read_xport("datafile.xpt")[0]
df.to_csv("datafile.csv", index=False)

# Coding

# SEQN = Respondent sequence number
# RIDAGEYR = Age in years at screening
# RIAGENDR = Gender
# DMDHREDU = Years of education completed

# extract seqn, age, gender and education where age > 50 < 85

df_filtered = df[(df['RIDAGEYR'] > 50) & (df['RIDAGEYR'] < 85)][['SEQN', 'RIDAGEYR', 'RIAGENDR', 'DMDEDUC2']]
df_filtered.head()
len(df_filtered)

df_filtered.to_csv("datafile_filtered.csv", index=False)


import re
from pathlib import Path

BASE_URL = "https://ftp.cdc.gov/pub/pax_g/"

def list_pax_filenames_matching_seqn(seqns, base_url=BASE_URL):
    """
    seqns: iterable of ints/strings (e.g., df_filtered['SEQN'])
    returns: list of matching filenames like ['62161.tar.bz2', ...]
    """
    # 1) Fetch directory listing (simple HTML index)
    r = requests.get(base_url, timeout=60)
    r.raise_for_status()
    
    html = r.text
    
    # 2) Extract filenames like 12345.tar.bz2 from the listing
    #    (matches href="62161.tar.bz2" or plain text occurrences)
    candidates = set(re.findall(r'(\d+\.tar\.bz2)', html))
    
    # 3) Normalize SEQN set (cast to int then back to str, to ignore any leading zeros)
    wanted = {str(int(x)) for x in seqns if pd.notna(x)}
    
    # 4) Keep candidates whose numeric stem matches a wanted SEQN
    matches = sorted(fn for fn in candidates if fn.split('.')[0] in wanted)
    return matches

# --- Example usage with your df_filtered ---
# df_filtered = ...  # your DataFrame
matches = list_pax_filenames_matching_seqn(df_filtered['SEQN'])
print(matches)
# write matches to text file
with open("matches.txt", "w") as f:
    for fn in matches:
        f.write(f"{fn}\n")




def download_matches(filenames, dest="pax_g_downloads", base_url=BASE_URL):
    """
    Stream-download matched tarballs to a local folder.
    """
    Path(dest).mkdir(parents=True, exist_ok=True)
    for fn in filenames:
        url = f"{base_url}{fn}"
        out = Path(dest) / fn
        with requests.get(url, stream=True, timeout=120) as resp:
            resp.raise_for_status()
            with open(out, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        # Optional: print or log each completed file
        # print(f"Downloaded {fn} -> {out}")

# Example:
# matches = list_pax_filenames_matching_seqn(df_filtered['SEQN'])
#download_matches(matches)
