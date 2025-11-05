# nightscout dash
A periodically updating dashboard to display continuous blood glucose statistics from nightscout deployment.

[Nightscout](https://nightscout.github.io/) is a tool to collect blood glucose data. Nightscout is compatabile with freestyle libre devices with the [Juggluco](https://github.com/j-kaltes/Juggluco) mobile app.

I use this with an old always on tablet on my wall to allow me to very easily see these values while exercising.  

## Installation
This tool is written in python. You can install this with pipx.

```
pipx install nightscout-dash
```

## Usage
I have an old nexus 10 android tablet obtained for about 30 dollars that I have attached to my wall with a tablet holder which connects to a server machine (which good easily be a raspberry pi) to display this graph.

Create user token in nightscout and create a credentials file, `credentials.json` like so:

```
{
    "user_token": "user-XXXX"
}
```

You can then run the dashboard with: `nigthscout-dash localhost:1024  nightscout.host --credential-file credential.json` and view this with `http://localhost:1024`.

I have ths URL open in a tabet. Nexus 10 tablets can, at the time or writing, be obtained for arond 30 dollars - though may have security issues.


## Cheaper continuous blood glucose monitors
Freestyle libre devices in the UK cost about three times the price of cheaper aidex devices which can be readily obtained through alibana and aliexpress.
In the US CGMs can be massivley overpriced. You can likely obtain an identical device on aliexpress for considerably cheaper.

Unfortunately the aidex 2 device (but not aidex 1) rqeuires a chinese phone number. I may reverse engineer these devices in the future to makes these devices more accessible and dramatically decrease the price of CGMs.


## Alternative and prior work
Nightscout can display similar information through its web interface. But this is designed for an always on dashvoard.
