# AdsPower Excel Form Filler

Yeh starter project is liye banaya gaya hai ke aap multiple AdsPower browser profiles parallel chala saken aur har browser Excel ki next pending row uthakar form fill kare.

## Yeh script kya karti hai

- Excel `.xlsx` file padhti hai.
- `DONE` rows ko skip karti hai.
- Multiple AdsPower profile IDs ko parallel run karti hai.
- Har worker browser ko next available row deti hai.
- Form fill karne ke baad Excel me status likh deti hai.

## Zaroori cheezen

1. AdsPower app machine par chal rahi ho.
2. Local API enabled ho.
3. Excel file band ho jab script run karein, warna save error aa sakta hai.
4. Python 3.11 ya close version installed ho.

## Setup

```powershell
cd D:\formfilling
python -m pip install -r requirements.txt
Copy-Item config.example.json config.json
```

## `config.json` me kya edit karna hai

- `adspower.profile_ids`: apne AdsPower browser/profile IDs dalo.
- `adspower.use_active_profiles`: agar aap AdsPower ke browsers pehle se khol dete ho to script un open browsers ko khud pick kar legi.
- `excel.path`: apni Excel file ka full path dalo.
- `form.use_existing_page`: agar aap AdsPower me form khud open karte ho to `true` rakho.
- `form.tab_index`: kis tab me form open hai. `-1` ka matlab last/opened tab.
- `form.url`: sirf tab dalo jab script khud har row par form URL khole.
- `fields`: Excel column names aur form selectors ko apne real form ke mutabiq verify karo.
- `form.submit_after_fill`: pehle test ke liye `false` rakho.
- `form.submit_selector`: jab test theek ho jaye tab actual submit button selector dalo aur `submit_after_fill` ko `true` karo.

## Run

```powershell
cd D:\formfilling
python form_filler.py --config config.json
```

Current config me `max_rows_per_profile = 1` hai, is liye har run me:

- Browser 1 first pending row lega, Browser 2 second pending row, aur isi tarah sequence me assignment hogi
- jitne AdsPower browsers open honge utni hi rows process hongi
- har browser sirf 1 row fill karega
- `Continue` click hoga
- Excel me us row ko `DONE` mark kiya jayega
- baqi rows next run ke liye pending rahengi

## Excel me auto columns

Script ye tracking columns khud add kar degi:

- `__status`
- `__message`
- `__processed_at`
- `__profile_id`

`DONE` wali rows dubara process nahi hongi.

## Aap ke screenshot ke hisaab se default mapping

Example config me yeh fields already daali hui hain:

- `First Name`
- `Last Name`
- `Street Address`
- `Zip Code`
- `Email Address`
- `Phone Number`
- `Birth Month`
- `Birth Day`
- `Birth Year`

Checkbox ka starter selector bhi diya gaya hai, lekin real page par kabhi kabhi checkbox hidden hota hai ya label click hota hai. Agar issue aaye to us selector ko exact inspect karke update karna hoga.

## Do mode

### 1. Aap form manually AdsPower me open karo

- `adspower.use_active_profiles = true`
- `adspower.profile_ids = []`
- `form.use_existing_page = true`
- `form.url = ""`
- Har AdsPower profile/browser me form page pehle se khol do.
- Script us browser ke configured tab me data bhar degi.

### 2. Script khud form khole

- `form.use_existing_page = false`
- `form.url = "https://your-real-form-url"`
- Har row par script form URL open karegi aur fill karegi.

## Important note

Abhi aap ne screenshot diya hai, lekin exact HTML selectors aur form URL nahi diye. Is liye yeh version ek configurable starter hai. Jaise hi aap form ka URL ya HTML/selectors doge, hum isay exact production version me tighten kar denge.
