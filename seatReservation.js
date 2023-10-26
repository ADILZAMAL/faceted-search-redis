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

async function main(){
    redisClient = createClient({
        url : 'redis://default:NWx4nx6BFhpnBF6hRBrb@localhost:6379'
    });
    redisClient.on('error', error => console.log("Redis client error", error))
    await redisClient.connect()
    testCreateSeat()
}

main()