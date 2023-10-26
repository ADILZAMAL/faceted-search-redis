const prefix = "";
const sep = ":";


function createKeyName (...vals){
    let start = prefix != "" ?  prefix + sep :  ""
    return start + vals.join(sep)
}

function incrChar(c){
    c = c.charCodeAt(0)
    if(c == 'Z')
     return 'A'
    else
     return String.fromCharCode(c + 1);
}

function incrStr(str){
    if(str.length == 0)
        return 'A';
    else{
        c = str[str.length - 1]; //Z
        str = str.slice(0, -1) // ''
        return incrStr(str) + (c == 'Z' ? 'A' :  incrChar(c))
    }
}

module.exports = {createKeyName, incrStr}