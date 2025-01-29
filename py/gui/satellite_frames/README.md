## Satellite frames - concept
This folder contains implementations of satellite frames.
Satellite frames differ from regular frames in the sense that they augment the GUI, while not being a direct component 
of it. It interacts with the GUI from a distance, much like how satellites interact with things on the ground, while 
being at quite a distance from the earth.

### Advantages
GUI components like satellite frames allow for distancing infrequently used components from the GUI, while having them 
as if they are a direct part of the GUI whenever they are needed.
This may allow for a much more elaborate settings page, for instance, as the frame itself is temporary and it will be 
gone after implementing the settings.

