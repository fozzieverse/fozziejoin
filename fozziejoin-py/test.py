import polars as pl
import fozzie

left = pl.DataFrame({"id": [1, 2, 3], "ARTS_FULL_NAME": ["JOHN SMITH", "JACK DOE", "SILLY BILLY"]})
right = pl.DataFrame({"key": [2, 3, 4], "CHILD_FULL_NAME": ["JON SMITH", "JACKSON DOE", "ZENDAYA"]})

joined = fozzie.fozzie_join(
    left, right, left_on='ARTS_FULL_NAME', right_on='CHILD_FULL_NAME', max_distance=0.9, q=3
)
print(joined)

