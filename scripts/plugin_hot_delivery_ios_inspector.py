"""Runs the isolation probe (case 7) in a plugin bundle page of the iOS simulator app, without Safari.

A minimal Web Inspector client over the simulator's webinspectord socket (the app's debug builds
let the inspector attach to a bundle page). Pass the socket of the simulator that runs the app:
`lsof -U | grep webinspectord_sim` names it next to that simulator's launchd_sim.

  plugin_hot_delivery_ios_inspector.py SOCKET list
  plugin_hot_delivery_ios_inspector.py SOCKET eval flare-plugin:// plugin_hot_delivery_probe.js \
      'flareIsolationProbe({...the options `manual isolation` prints...})' > probe-ios.json

`eval` loads the file into the page whose address contains the given text, evaluates the
expression, awaits it when it is a promise and prints its JSON value.
"""
import glob, json, plistlib, socket, struct, sys, uuid

BUNDLE = "com.flare.social.example.app"


class Wir:
    def __init__(self):
        path = sys.argv.pop(1) if sys.argv[1].startswith("/") else glob.glob("/private/var/tmp/com.apple.launchd.*/com.apple.webinspectord_sim.socket")[0]
        self.s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.s.connect(path)
        self.s.settimeout(20)
        self.conn = str(uuid.uuid4()).upper()
        self.sender = str(uuid.uuid4()).upper()
        self.buf = b""
        self.apps, self.listings = {}, {}

    def send(self, selector, argument):
        argument = dict(argument, WIRConnectionIdentifierKey=self.conn)
        data = plistlib.dumps({"__selector": selector, "__argument": argument}, fmt=plistlib.FMT_BINARY)
        self.s.sendall(struct.pack(">I", len(data)) + data)

    def recv(self):
        while len(self.buf) < 4 or len(self.buf) < 4 + struct.unpack(">I", self.buf[:4])[0]:
            chunk = self.s.recv(65536)
            if not chunk:
                raise EOFError
            self.buf += chunk
        size = struct.unpack(">I", self.buf[:4])[0]
        message, self.buf = plistlib.loads(self.buf[4:4 + size]), self.buf[4 + size:]
        selector, argument = message.get("__selector"), message.get("__argument", {})
        if selector in ("_rpc_reportConnectedApplicationList:",):
            self.apps.update(argument.get("WIRApplicationDictionaryKey", {}))
        elif selector == "_rpc_applicationConnected:":
            self.apps[argument["WIRApplicationIdentifierKey"]] = argument
        elif selector == "_rpc_applicationSentListing:":
            self.listings[argument["WIRApplicationIdentifierKey"]] = argument.get("WIRListingKey", {})
        return selector, argument

    def pump(self, until, limit=200):
        for _ in range(limit):
            selector, argument = self.recv()
            if until(selector, argument):
                return selector, argument
        raise TimeoutError

    def app(self):
        self.send("_rpc_reportIdentifier:", {})
        try:
            self.pump(lambda s, a: any(v.get("WIRApplicationBundleIdentifierKey") == BUNDLE for v in self.apps.values()), 20)
        except (TimeoutError, socket.timeout):
            pass
        for key, value in self.apps.items():
            if value.get("WIRApplicationBundleIdentifierKey") == BUNDLE:
                return key
        raise SystemExit("app not inspectable: " + json.dumps({k: v.get("WIRApplicationBundleIdentifierKey") for k, v in self.apps.items()}))

    def pages(self, app):
        self.send("_rpc_forwardGetListing:", {"WIRApplicationIdentifierKey": app})
        self.pump(lambda s, a: s == "_rpc_applicationSentListing:" and a.get("WIRApplicationIdentifierKey") == app)
        return self.listings[app]

    def attach(self, app, page):
        self.app_id, self.page = app, page
        self.send("_rpc_forwardSocketSetup:", {"WIRApplicationIdentifierKey": app, "WIRPageIdentifierKey": page,
                                               "WIRSenderKey": self.sender, "WIRAutomaticallyPause": False})
        self.target = None
        self.next_id = 0
        try:
            self.pump(lambda s, a: s == "_rpc_applicationSentData:" and self._event(a), 30)
        except (TimeoutError, socket.timeout):
            pass

    def _event(self, argument):
        message = json.loads(argument["WIRMessageDataKey"])
        if message.get("method") == "Target.targetCreated":
            info = message["params"]["targetInfo"]
            if info.get("type") == "page":
                self.target = info["targetId"]
                return True
            self.target = self.target or info["targetId"]
        return False

    def call(self, method, params):
        self.next_id += 1
        inner_id = self.next_id
        inner = {"id": inner_id, "method": method, "params": params}
        if self.target:
            self.next_id += 1
            outer = {"id": self.next_id, "method": "Target.sendMessageToTarget",
                     "params": {"targetId": self.target, "message": json.dumps(inner)}}
        else:
            outer = inner
        self.send("_rpc_forwardSocketData:", {"WIRApplicationIdentifierKey": self.app_id,
                                              "WIRPageIdentifierKey": self.page, "WIRSenderKey": self.sender,
                                              "WIRSocketDataKey": json.dumps(outer).encode()})
        while True:
            selector, argument = self.recv()
            if selector != "_rpc_applicationSentData:":
                continue
            message = json.loads(argument["WIRMessageDataKey"])
            if message.get("method") == "Target.dispatchMessageFromTarget":
                message = json.loads(message["params"]["message"])
            if message.get("id") == inner_id:
                return message


def main():
    w = Wir()
    app = w.app()
    pages = w.pages(app)
    if sys.argv[1] == "list":
        for key, page in pages.items():
            print(key, page.get("WIRTypeKey"), page.get("WIRURLKey"), page.get("WIRTitleKey"))
        return
    _, needle, source_file, expression = sys.argv[1:5]
    page = next(p["WIRPageIdentifierKey"] for p in pages.values() if needle in (p.get("WIRURLKey") or ""))
    w.attach(app, page)
    source = open(source_file).read()
    loaded = w.call("Runtime.evaluate", {"expression": source + "\n;window.flareIsolationProbe = flareIsolationProbe; 'ok'",
                                         "returnByValue": True})
    print("load", json.dumps(loaded)[:300], file=sys.stderr)
    started = w.call("Runtime.evaluate", {"expression": expression, "returnByValue": False})
    result = started.get("result", {}).get("result", {})
    if result.get("objectId") and result.get("className") == "Promise":
        w.s.settimeout(120)
        awaited = w.call("Runtime.awaitPromise", {"promiseObjectId": result["objectId"], "returnByValue": True})
        print(json.dumps(awaited.get("result", {}).get("result", {}).get("value")))
    else:
        print(json.dumps(started))


if __name__ == "__main__":
    main()
