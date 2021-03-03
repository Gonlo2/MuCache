![](/assets/logo.readme.png?raw=true "mucache logo")

# Introduction

A multimedia cache to allow view remote multimedia files in a easy, low-cost and automated way.

## How it works

* Ask for a token to [power_manager](https://github.com/Gonlo2/power_manager) when the remote server need be accesed.
* Store the remote files structure/attributes in a local DB to view it when the remote server is offline
* When the remote file need be accesed it do a passthrow to remote file until a prolonged use of the file is detected, at this point it start caching the file and use the cache copy whenever possible.
* To avoid turning the server on and off all the time when watching several short episodes, a prefetch of the following files is made until a viewing time of X minutes is obtained.
* In order to know when a file has been added/changed/modified, [reinotify](https://github.com/Gonlo2/reinotify) is used together to notify mucache of these changes and redirect the request to the upper layer if necessary (for example this modified [minidlna](https://github.com/Gonlo2/minidlna)).

## Credits

Created and maintained by [@Gonlo2](https://github.com/Gonlo2/).

## Third party libraries

* PyExifTool: https://github.com/smarnach/pyexiftool
* fusepy: https://github.com/fusepy/fusepy
* power_manager: https://github.com/Gonlo2/power_manager
* reinotify: https://github.com/Gonlo2/reinotify

## License

This project is licensed under the GNU General Public License v2.0 - see the [LICENSE](LICENSE) file for details
