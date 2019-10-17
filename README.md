#  Update Zip And Email Xlsx Files

This script execute sum queries on postgres DB and update sheets of existing xlsx files by query values and zip all files in the one zip file and send it by email.
## Installation

Use the package manager [mvn](https://www.javahelps.com/2017/10/install-apache-maven-on-linux.html) to install.

Run following commands in directory that the pom.xml exists.
```bash
mvn install
mvn package
```
And run the bash script.
```bash
sh mci_management_report.sh
```
So it executes according to the variables and settings inside the 
 bash script.

## Usage

```python
import foobar

foobar.pluralize('word') # returns 'words'
foobar.pluralize('goose') # returns 'geese'
foobar.singularize('phenomena') # returns 'phenomenon'
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
