const prefix = "";
const sep = ":";


function createKeyName (...vals){
    let start = prefix != "" ?  prefix + sep :  ""
    return start + vals.join(sep)
}
module.exports = {createKeyName}