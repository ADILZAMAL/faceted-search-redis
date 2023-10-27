const {createClient, RedisFlushModes} = require('redis')
const {createKeyName, incrStr} = require('./Utils/keynameHelper')
const {v4} = require('uuid')

let redisClient;

async function createEvent(eventSku, blocks=2, SeatsPerBlock=32, tier="General"){
    blockNumber = 1
    for(let i = 0; i < blocks; i++){
        key = createKeyName("seatmap", eventSku, tier, blockNumber)
        val = Math.pow(2, SeatsPerBlock) - 1
        await redisClient.sendCommand(['BITFIELD', key, "SET", "u32", 0+'', val.toString()])
        blockNumber += 1;
    }
}

async function getEventSeatBlock(eventSku, tier, blockName){
    // For the given event, tier & block return the seat map
    key = createKeyName('seatmap', eventSku, tier, blockName)
    return redisClient.sendCommand(['BITFIELD', key, 'GET', 'u32', 0 + ''])
}

async function setSeatMap(eventSku, tier, blockNumber, seatMap){
    // Set the seat map to given value
    key = createKeyName('seatmap', eventSku, tier, blockNumber)
    redisClient.sendCommand(['BITFIELD', key, 'SET', 'u32', 0 + '', seatMap.toString()])
}

async function printSeatMap(eventSku, tier="*"){
    // Format the seatMap for display purpose.
    key = createKeyName("seatmap", eventSku, tier)
    for await (const block of redisClient.scanIterator({MATCH: key, COUNT: 1000})){
        [_, _, tier, blockNumber] = block.split(':')
        seatMap = await getEventSeatBlock(eventSku, tier, blockNumber)
        // console.log(block)
        seatMap = seatMap[0]
        seatMapView = block.toString() + " | "
        while(seatMap){
            // console.log(seatMap)
            seatMapView = seatMapView + (seatMap & 1)
            seatMap >>>= 1 
        }
        seatMapView = seatMapView + ' |'
        console.log(seatMapView)
    } 

}

async function testCreateSeat(){
    console.log("Test - Create & Print seat map")
    console.log("Create 2 blocks of 10 seats")
    eventSku = "123-ABC-723"
    await createEvent(eventSku, 2, 10)
    await printSeatMap(eventSku)
}

function getAvailable(seatMap, seatRequired){
    seatsArrangment = []
    totalSeats = 10;
    if (seatRequired <= 10){
        requiredBlock = Math.pow(2, seatRequired) - 1;
        for(let i = 1; i <= totalSeats; i++){
            if((seatMap & requiredBlock) == requiredBlock){
                seatsArrangment.push({'firstSeat': i, 'lastSeat': i + seatRequired - 1})
            }
            requiredBlock = requiredBlock << 1
        }
    }
    return [...seatsArrangment]
}

async function findSeatSelection(eventSku, tier, seatsRequired){
    // Find seats ranges that meet the criteria
    // Get all the seat row
    seats = []
    key = createKeyName("seatmap", eventSku, tier, "*")
    for await (const block of redisClient.scanIterator({MATCH: key, COUNT: 1000})){
        bitcount = await redisClient.bitCount(block)
        if(bitcount < seatsRequired){
            console.log(`Row ${block} doesn't have enough seats.`)
        }else{
            [_, _, tier, blockNumber] = block.split(':')
            seatMap = await getEventSeatBlock(eventSku, tier, blockNumber)
            let blockAvailability = getAvailable(seatMap, seatsRequired)
            if (blockAvailability.length > 0 ){
                const obj =  {'event': eventSku, 'tier': tier, 'block': blockNumber, 'available': [...blockAvailability]}
                seats.push(obj)
            }
        }
    }
    return seats;
}

async function testFindSeat(){
    // Test function to find various combinations of seats.
    console.log("Test - Find Seats")
    eventSku = "123-ABC-723"
    await createEvent(eventSku, 2, 10)

    console.log("Find 6 contiguous available seats")
    availableSeats = await findSeatSelection(eventSku, "General", 6)
    for(const obj of availableSeats){
        console.log(obj)
    }

    // Check that we skip rows
    console.log("Remove a 4 seat from Block 1, so only Block 2 has the right availability for 6 seats")
    // unset bit from 2-5
    setSeatMap(eventSku, "General", '1', Math.pow(2, 10) - 31)
    printSeatMap(eventSku)
    availableSeats = await findSeatSelection(eventSku, "General", 6)
    for(const obj of availableSeats){
        console.log(obj)
    }
}

async function main(){
    redisClient = createClient({
        url : 'redis://default:NWx4nx6BFhpnBF6hRBrb@localhost:6379'
    });
    redisClient.on('error', error => console.log("Redis client error", error))
    await redisClient.connect()
    // testCreateSeat()
    testFindSeat()
}

main()