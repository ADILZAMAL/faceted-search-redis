const {createClient} = require('redis');
const {createHash} = require('crypto')
const keynameHelper = require('./Utils/keynameHelper');

let redisClient;

const events= [
{
    sku: "123-ABC-723",
    name: "Men's 100m Final",
    disabled_access: true,
    medal_event: true,
    venue: "Olympic Stadium",
    category: "Track & Field"
},
{
    sku: "737-DEF-911",
    name: "Women's 4x100m Heats",
    disabled_access: true,
    medal_event: false,
    venue: "Olympic Stadium",
    category: "Track & Field"
},
{   sku: "320-GHI-921",
    name: "Womens Judo Qualifying",
    disabled_access: false,
    medal_event: false,
    venue: "Nippon Budokan",
    category: "Martial Arts"
}
]


async function createEvent(events){
    // Create events from the passed array.
    for (let i = 0; i < events.length; i++)     
    {
        let key = keynameHelper.createKeyName("event", events[i]['sku'])
        await redisClient.set(key, JSON.stringify(events[i]))
    }
}

const lookUpAttr = ['disabled_access', 'medal_event', 'venue']
//event:
// fs:venue:"Olympic Stadium"  event:[sku]
async function createEventWithLookUp(events){
    //For each attribute & value combination, add the event into set
    await createEvent(events) //set "event:"320-GHI-921"" events[2]

    for(let i = 0; i < events.length; i++){
        for(let j = 0; j < lookUpAttr.length; j++){
            attr = lookUpAttr[j];
            if(events[i].hasOwnProperty(attr)){
                fsKey = keynameHelper.createKeyName("fs", attr, events[i][attr])
                await redisClient.sAdd(fsKey, events[i]['sku'])
            }
        }
    }
}

async function matchByInspection(...keys){
    let matches = []
    let searchKey = keynameHelper.createKeyName("event", "*")
    for await (const key of redisClient.scanIterator({MATCH: searchKey, TYPE: 'string'})){
        let event = await redisClient.get(key);
        event = JSON.parse(event)
        let match = false
        for(let i = 0; i < keys.length; i++)
        {
            let searchKey = keys[i][0]
            let searchVal = keys[i][1]
            if ((event.hasOwnProperty(searchKey)) && (event[searchKey] == searchVal))
            match = true
        else{
            match = false;
            break;
        }
    }
    if (match)
    matches.push(event['sku'])
}
    return matches
}

async function matchByFaceting(...keys){
    let facets = []
    for(let i = 0; i < keys.length; i++){
        let attr = keys[i][0]
        let attrVal = keys[i][1]
        let fsSearchKey = keynameHelper.createKeyName("fs", attr, attrVal)
        facets.push(fsSearchKey)
    }
    return redisClient.sInter(facets)
}

async function testObjectInspection(){
    console.log("\n Testing Method 1: Object Inspection")
    await createEvent(events)

    // Find the match (disabled_access=true)
    console.log("\ndisabled_access=true")
    matches = await matchByInspection(['disabled_access', true])
    for(let i = 0; i < matches.length; i++){
        console.log("\n" + matches[i])
    }

    // Find the match (disabled_access=true, medal_event=false)
    console.log("\ndisabled_access=true, medal_event=false")
    matches = await matchByInspection(['disabled_access', true], ['medal_event', false])
    for(let i = 0; i < matches.length; i++)
        console.log("\n" + matches[i])

    // Find the match (disabled_access=false, medal_event=false, venue='Nippon Budokan')
    console.log("\ndisabled_access=false, medal_event=false, venue=Nippon Budokan")
    matches = await matchByInspection(['disabled_access', false], ['medal_event', false], ['venue', 'Nippon Budokan'])
    for(let i = 0; i < matches.length; i++)
        console.log("\n"+ matches[i])
}

async function test_faceted_search(){
    await createEventWithLookUp(events)
    // Test function for Method 2: Faceted Search
    console.log("\n Testing Method 2: Faceted Search")

    //Find the match (disabled_access=true)
    console.log("\ndisabled_access=true")
    matches = await matchByFaceting(['disabled_access', true])
    for(let i = 0; i < matches.length; i++)
    console.log("\n" + matches[i])

    //Find the match (disabled_access=true, medal_event=false)
    console.log("\ndisabled_access=true, medal_event=false")
    matches = await matchByFaceting(['disabled_access', true], ['medal_event', false])
    for(let i = 0; i < matches.length; i++)
    console.log("\n" + matches[i])

    //Find the match (disabled_access=false, medal_event=false, venue='Nippon Budokan')
    console.log("\ndisabled_access=false, medal_event=false, venue=Nippon Budokan")
    matches = await matchByFaceting(['disabled_access', false], ['medal_event', false], ['venue', 'Nippon Budokan'])
    for(let i = 0; i < matches.length; i++)
    console.log("\n" + matches[i])
}


// Method 3 : Hashed Faceted Search

async function createEventHashedLookup(events){
    // await createEvent(events)
    for(let i = 0; i < events.length; i++){
        hfs = []
        for(let j = 0; j < lookUpAttr.length; j++){
            if(events[i].hasOwnProperty(lookUpAttr[j])){
                hfs.push(lookUpAttr[j], events[i][lookUpAttr[j]])
            }
            let x = hfs
            let hashedVal = createHash('sha256').update(x.join("")).digest('hex')
            hfsKeyname = keynameHelper.createKeyName("hfs", hashedVal)
            redisClient.sAdd(hfsKeyname, events[i]['sku'])
        }
    }
}

async function matchByHashFaceting(...keys){
    let facets = []
    let matches = []
    for(let i = 0; i < lookUpAttr.length; i++){
        let lookUpKey = lookUpAttr[i]

        for(let j = 0; j < keys.length; j++){
            if(lookUpKey == keys[j][0])
            facets.push(keys[j][0], keys[j][1])
        }
    }
    hashedVal = createHash('sha256').update(facets.join("")).digest('hex')
    hashedKey = keynameHelper.createKeyName("hfs", hashedVal)
    for await (const member of redisClient.sScanIterator(hashedKey, {COUNT: 1000})){
        matches.push (member)
    }

    return matches;
}

async function testHashedFacetedSearch(){
    await createEventHashedLookup(events)
    console.log("\n Testing Method3: Hashed Faceted Search")

    //Find the match (disabled_access=true)
    console.log("\ndisabled_access=true")
    matches = await matchByHashFaceting (['disabled_access', true])
    for(let i = 0; i < matches.length; i++)
    console.log("\n" + matches[i])

    // Find the match (disabled_access=true, medal_event=false)
    console.log("\ndisabled_access=true, medal_event=false")
    matches = await matchByHashFaceting(['disabled_access', true], ['medal_event', false])
    for(let i = 0; i < matches.length; i++)
    console.log("\n" + matches[i])

    // Find the match (disabled_access=false, medal_event=false, venue='Nippon Budokan')
    console.log("\ndisabled_access=false, medal_event=false, venue=Nippon Budokan")
    matches = await matchByHashFaceting(['disabled_access', false], ['medal_event', false], ['venue', 'Nippon Budokan'])
    for(let i = 0; i < matches.length; i++)
    console.log("\n" + matches[i])
}

(async () => {
    redisClient = createClient({
        url : 'redis://default:NWx4nx6BFhpnBF6hRBrb@localhost:6379'
    });
    redisClient.on('error', err => console.log('Redis client Error ', err))
    await redisClient.connect()
    await testObjectInspection() // Time complexity = Nscan + Nget
    await test_faceted_search() // Time complexity = min{cardinality of all the sets} * Total number of sets  => This is best suited when data distribution in one set is les compared to other and number of sets is less.
    await testHashedFacetedSearch(); // Time complexity = cardinality of the set matched for the given attribute values combination
})();