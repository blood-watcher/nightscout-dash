# nightscout dash
A periodically updatin dashboard to display glucose statistics from nightscout deployment.

I use this with an old always on tablet on my wall to allow me to very easily see these values exercising.  Nightscout is compatabile with freestyle libre devices with the [Juggluco](https://github.com/j-kaltes/Juggluco) mobile app.


## Installation
This tool is written in python. You can install this with pipx.

```
pipx install nightscout-dash
```

## Usage
Create  user token in nightscout and create a credentials file, `credentials.json` like so:


```
{
    "user_token": "user-XXXX"
}
```

You can then run the dashboard with: `nigthscout-dash localhost:1024  nightscout.host --credential-file credential.json` and view this with `http://localhost:1024`.

I have ths URL open in a tabet. Nexus 10 tablets can, at the time or writing, be obtained for arond 30 dollars - though may have security issues.


## Cheaper continuous blood glucose monitors.
Freestyle libre devices in the UK cost about three times the price of cheaper aidex devices which can be readily obtained through alibana and aliexpress.
In the US CGMs can be massivley overpriced. You can likely obtain an identical device on aliexpress for considerably cheaper.

Unfortunately the aidex 2 device (but not aidex 1) rqeuires a chinese phone number. I may reverse engineer these devices in the future to makes these devices
more accessible and increase access.
