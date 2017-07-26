from vionmodels import VionContainer
from carddeck.io.input_output import read_ucis
import unittest

class TestUCIModel(unittest.TestCase):

    def test_load_uci(self):
        DATAFOLDER = "/home/vionlabs/Documents/gitlab/stories/insight-cards/carddeck/data/cmore_week/"
        USERFILE = DATAFOLDER +"cmore_user.json"
        CONTENTFILE = DATAFOLDER + "cmore_content_full.json"
        INTERACTIONFILE = DATAFOLDER + "cmore_interaction.json"

        ucis = read_ucis(USERFILE, CONTENTFILE, INTERACTIONFILE)
        uci = ucis[44]
        print ''
        print 'this is userId, ', uci.user.userId
        print 'this is imdbID, ', uci.content.externalIDs.imdbID
        print 'this is runtime, ', uci.content.runtime
        print 'this is interaction duration, ', uci.interaction.interactionDurationSeconds
        print 'this is device, ', uci.interaction.device
        print 'this is first event, ', uci.interaction.firstEvent
        print uci.as_dict()



if __name__ == "__main__":
    unittest.main()
