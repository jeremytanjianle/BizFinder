# BizFinder
Search for Businesses / Branches in SG with Google Maps, WIPO and Singapore Business Directory (SGBD)

### 1. Google Maps
Search for (single) places directly on Google Maps or for multiple branches on Google Maps.  
```
import BizFinder

# search Google Maps
maps = BizFinder.gmaps()
maps.get_entity('hard rock cafe cuscaden road', visits = False)

# if youre looking for more branches
# run a more exhaustive search with a webdriver
branches, driver = maps.search_estab('hard rock cafe')
```

### 2. WIPO Brand Databse
Otherwise, BizFinder search WIPO for intellectula property holders in SG
```
wipo = BizFinder.WIPO()
wipo.holdingcoy2brand('BAKEMATRIX')
```
  
    
__Disclaimer__  
###### Please scrape responsibly.  
###### Repository for sharing purposes. All data presented come from other 3rd party websites.  
###### No responsibiliy will be held for any mis-information whatsoever.  
