const {createClient} = require('redis')
const {createKeyName} = require('./Utils/keynameHelper')
const {v4} = require('uuid')

let redisClient;

const events = [
    {'sku': "123-ABC-723",
'name': "Men's 100m Final",
'disabled_access': "True",
'medal_event': "True",
'venue': "Olympic Stadium",
'category': "Track & Field",
'capacity': 60102,
'available:General': 20000,
'price:General': 25.00
},
{'sku': "737-DEF-911",
'name': "Women's 4x100m Heats",
'disabled_access': "True",
'medal_event': "False",
'venue': "Olympic Stadium",
'category': "Track & Field",
'capacity': 60102,
'available:General': 10000,
'price:General': 19.50
},
{'sku': "320-GHI-921",
'name': "Womens Judo Qualifying",
'disabled_access': "False",
'medal_event': "False",
'venue': "Nippon Budokan",
'category': "Martial Arts",
'capacity': 14471,
'available:General': 5000,
'price:General': 15.25
}
]

async function createEvent(events, available = undefined, price = undefined, tier="General"){
    // If available seats and price are given override it
    for(let i = 0; i < events.length; i++){
        if(available != undefined)
            events[i]["available:" + tier] = available
        if(price != undefined)
            events[i]["price:" + tier] = price
        key = createKeyName("event", events[i]['sku'])
        await redisClient.hSet(key, events[i])
        // await redisClient.set(key, events[i]['sku'])
    }
}

async function checkAvailabilityAndPurchase(customer, eventSku, qty, tier="General"){
    // Check if there is sufficient inventory before making the purchase
    p = redisClient.multi()
    try{
        key = createKeyName("event", eventSku)
        await redisClient.watch(key)
        available = parseInt(await redisClient.hGet(key, "available:" + tier), 10)
        price = parseFloat(await redisClient.hGet(key, "price:" + tier))
        if(available >= qty){
            p.hIncrBy(key, "available:" + tier, -qty)
            orderId = v4()
            purchase = {'order_id': orderId, 'customer': customer,
                        'tier': tier, 'qty': qty, 'cost': qty * price,
                        'event_sku': eventSku, 'ts': Date.now()}
            soKey = createKeyName("sales_order", orderId)
            p.hSet(soKey, purchase)
            p.exec()
        }
        else
            console.log(`Insufficient inventory, have ${available}, requested ${qty}`)
    }
    catch(err){
        console.log("Write conflict check availability and purchase:", err)
    }
    finally{
        p.reset()
    }

    console.log("Purchase complete!")
}

async function printEventDetails(eventSku){
    // Print the details of the event based on the event sku passed
    key = createKeyName("event", eventSku)
    console.log(await redisClient.hGetAll(key))
}

async function testCheckAndPurpose(){
    // Test function check & purchase method
    console.log("\nTest 1: Check Stock available & purchase")
    // create events with 10 ticket available each event
    await createEvent(events, 10)

    // Stock available
    console.log("\nRequest 5 ticket, success")
    requestor = "bill"
    eventRequested="123-ABC-723"
    await checkAvailabilityAndPurchase(requestor, eventRequested, 5)
    await printEventDetails(eventRequested)

    // No purchase, not enough stock
    console.log("\nRequest 6 ticket, failure because of insufficient inventory")
    requestor = "mary"
    eventRequested = "123-ABC-723"
    await checkAvailabilityAndPurchase(requestor, eventRequested, 6)
    await printEventDetails(eventRequested)
}

function creditAuth(customer, price){
// Test function to approve/deni an authorization request
    if (customer.toUpperCase() == "JOAN")
        return false
    else
    return true
}

async function reserve(customer, eventSku, qty, tier="General"){
// First reserve the inventory and perform a credit authorization. If successful
// then confirm the invetory deduction or back out the deduction.
    p = redisClient.multi()
    try{
        key = createKeyName("event", eventSku)
        await redisClient.watch(key)
        available = parseInt(await redisClient.hGet(key, "available:"+tier), 10)
        price = parseFloat(await redisClient.hGet(key, "price:"+tier))
        if(available >= qty){
            orderId = v4()
            timestamp = Date.now()
            p.hIncrBy(key, "available:" + tier, -qty)
            p.hIncrBy(key, "held:" + tier, qty)
            // create a hash to store seat hold info
            holdKey = createKeyName("ticket_hold", eventSku)
            console.log(holdKey)
            p.hSetNX(holdKey, "qty:"+orderId, tier)
            p.hSetNX(holdKey, "tier:" + orderId, tier)
            p.hSetNX(holdKey, "ts:" + orderId, timestamp.toString())
            p.exec()
        }
        else
        console.log(`Seats not available, have ${available}, requested ${qty}`)
    }
    catch(err){
        console.log("Write conflict in reserve:", err)
    }

    if (creditAuth(customer, qty * price)){
        try{
            p = redisClient.multi()
            purchase={'order_id': orderId, 'customer': customer,
            'tier': tier, 'qty': qty, 'cost': qty * price,
            'event_sku': eventSku, 'ts': timestamp}

            redisClient.watch(key)
            // Remove the seat hold, since it is no longer neede
            p.hDel(holdKey, "qty:"+orderId)
            p.hDel(holdKey, "tier:"+orderId)
            p.hDel(holdKey, "ts:"+orderId)
            //Update the event
            p.hIncrBy(key, "held:"+tier, -qty)
            // Post the sales order
            soKey = createKeyName("sales_order", orderId)
            p.hSet(soKey, purchase)
            p.exec()
        }
        catch(err){
            console.log("\nWrite conflict in reserve:", err)
        }
        console.log("\nPurchase complete!")
    }
    else{
        console.log(`\nAuth failed on order ${orderId} for customer ${customer} for price ${price * qty}`)
        backOutHold(eventSku, orderId)
    }
}

async function testReserve(){
// Test function reserve  & credit auth
console.log("\n Test 2: Reserve stock, perform auth and complete purchase")
// Create events with 10 tickets available
await createEvent(events, 10)

// Make purchase 
console.log("\nReserve & purchase 5 seats")
requestor = "jamie"
eventRequested = "737-DEF-911"
await reserve(requestor, eventRequested, 5)
await printEventDetails(eventRequested)
}

async function main(){
    redisClient = createClient({
        url : 'redis://default:NWx4nx6BFhpnBF6hRBrb@localhost:6379'
    });
    redisClient.on('error', error => console.log("Redis client error", error))
    await redisClient.connect();
    // testCheckAndPurpose()
    await testReserve()
}


main()