## For flatdb
{.emit: """

function a2hex(str) {
  var arr = [];
  for (var i = 0, l = str.length; i < l; i ++) {
    var hex = Number(str.charCodeAt(i)).toString(16);
    arr.push(hex);
  }
  return arr.join('');
}

function makeid() {
  var text = "";
  var possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

  for (var i = 0; i < 12; i++)
    text += possible.charAt(Math.floor(Math.random() * possible.length));

  return text;
}
""".}


when not defined(nodejs):
  {.emit: """
   function jsLoad(db) {
    return window.localStorage.getItem(db)
  }

  function jsStore(db, data) {
    window.localStorage.setItem(db, data)
  }

  function jsRemove(db) {
    // window.localStorage.setItem(db, null)
    window.localStorage.removeItem(db)
  }  

  function jsCopy(src, dst) {
    // window.localStorage.setItem(db, null)
    window.localStorage[dst] = window.localStorage[src]
  }  

  function jsAppend(db, line) {
    // window.localStorage.setItem(db, null)
    if (window.localStorage[db] == undefined) { 
      jsStore(db, line + "\n");
    } else {
      window.localStorage[db] += line + "\n"
    }

  }     

  """.}

when defined(nodejs):
  {.emit: """

  fs = require("fs")

  function jsLoad(db) {
    return fs.readFileSync(db).toString()
  }

  function jsStore(db, data) {
    fs.writeFileSync(db, data)
  }

  function jsRemove(db) {
    fs.unlinkSync(db)
  }  

  function jsCopy(src, dst) {
    fs.createReadStream(src).pipe(fs.createWriteStream(dst))
  }  

  function jsAppend(db, line) {
    fs.appendFileSync(db, line + "\n")
  }        

  """.}

proc jsLoad*(db: cstring): cstring {.importc.}
proc jsLoad*(db: string): string = return $jsLoad(db.cstring) 

proc jsStore*(db, data: cstring): cstring {.importc.}
proc jsStore*(db, data: string): string = return $jsStore(db.cstring, data.cstring) 

proc jsRemove*(db: cstring) {.importc.}
proc jsRemove*(db: string) = jsRemove(db.cstring)

proc jsCopy*(src, dst: cstring) {.importc.}
proc jsCopy*(src, dst: string) = jsCopy(src.cstring, dst.cstring)


proc jsAppend*(db, line: cstring) {.importc.}  
proc jsAppend*(db, line: string) = jsAppend(db.cstring, line.cstring)


proc a2hex*(str: cstring): cstring {.importc.}
proc makeid*(): cstring {.importc.}


proc genOid*(): string =
  return $a2hex(makeid())
