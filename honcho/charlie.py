import random
from datetime import datetime, timedelta

from honcho.models import *
from honcho.repository import *
from honcho.services import *
from honcho.exceptions import *
import honcho.util as util

CHARLIE_QUOTES = [
    "I eat stickers all the time, dude"
,   "You're telling me that you believe that Christ comes back to life every Sunday in the form of a bowl of crackers and you proceed to just eat the man?"
,   "What is this word 'spa'? I feel like you're starting to say a word and you're not finishing it. Are you trying to say 'Spagetti'? Are you taking me for a spaghetti day?"
,   "Just get a job? Why don't I strap on my job helmet and squeeze down a job cannon and fire off into jobland, where jobs grow on jobbies!"
,   "Alright well I'm gonna check it out anyway, there could be something delicious in here that wasps do make and I want that."
,   "If animals have taught me anything, It's that you can easily die and very quickly under a bus and on the side of the road."
,   "Of course there's gonna be an explosion. You think I'm not gonna explode?"
,   "Cannibalism? Racism? Dude, that's not for us...those decisions are better left to the suits in Washington..."
,   "You know what dude hear me out for a second okay. Now technically that strain did appear to me. Also, I am familiar with carpentry and I don't know who my father is. So, am I the messiah? I don't know, I could be, I'm not ruling it out."
,   "I'm gonna kick some butt, I'm gonna drive a big truck, I'm gonna rule this world...I'm gonna rise up...ROCK, FLAG, and EAGLE!"
,   "Well, when I showed up this morning I didn't have a formal resume on me so I was sort of hoping the photograph of Mr. Jenner could represent the standard of excellence I'm hoping to bring to this position."
,   "I'll take that advice under cooperation, alright? Let's say you and I go toe-to-toe on bird law and see who comes out the victor."
,   "Look, buddy. I know a lot about the law and the various other lawerings. I'm well educated. Well versed. I know that situations like this-real estate wise-they're very complex."
,   "That lawyer guy, okay? He totally besmirched me today, and I demand satisfaction from him."
,   "Take a look at this picture...No, dude. They're dueling, okay? These are lawyers settling an argument by dueling it out."
,   "As far as we're concerned, your honor, those ballots could have been for Mickey Mouse."
,   "Oh well, we are all hungry...Let's talk about the contact here."
,   "Although really I specialize in bird law. So I don't wanna mess around."
,   "Oh My God! I will smash your face into -into a jelly."
,   "Well, I'm the bad guy then."
,   "All right look at this, sometimes you've got to crack a few eggs to make an omelet."
,   "I'm cracking eggs of wisdom."
]

from datetime import datetime, timedelta

from honcho.models import *
from honcho.repository import *
from honcho.services import *
from honcho.exceptions import *
import honcho.util as util

logger = util.get_logger('charlie')
class Charlie:
    
    DEFAULT_POLLING_WAIT = 5 # seconds
    DEFAULT_WORKER_TIMEOUT = 300 # seconds
    
    def __init__(self, worker_service, work_item_service, time_service, poll_wait=None, worker_time_out=None):
        if self.poll_wait is None:
            poll_wait = self.DEFAULT_POLLING_WAIT
        if worker_time_out is None:
            worker_time_out = self.DEFAULT_WORKER_TIMEOUT
            
        self.worker_service = worker_service
        self.work_item_service = work_item_service
        self.time_service = time_service
        self.poll_wait = poll_wait
        self.worker_time_out = worker_time_out
        self.counters = None
        
    def step(self):
        
    
    def run(self):
        quote = random.choice(self.CHARLIE_QUOTES)
        logger.info('waking up Charlie...')
        logger.info(f" - {quote}")
        while True:
            self.step()
            self.time_service.sleep(self.poll_wait)