#Run doctests
python groupby.py
python iterators.py

cd test/
nosetests --with-doctest --verbosity 3
cd ..
