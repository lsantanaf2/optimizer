import unittest
from unittest.mock import MagicMock, patch
import sys
import json

# Mock facebook_business modules BEFORE importing meta_api
sys.modules['facebook_business'] = MagicMock()
sys.modules['facebook_business.api'] = MagicMock()
sys.modules['facebook_business.adobjects'] = MagicMock()
sys.modules['facebook_business.adobjects.adaccount'] = MagicMock()
sys.modules['facebook_business.adobjects.adimage'] = MagicMock()
sys.modules['facebook_business.adobjects.advideo'] = MagicMock()
sys.modules['facebook_business.adobjects.adcreative'] = MagicMock()
sys.modules['facebook_business.adobjects.ad'] = MagicMock()
sys.modules['facebook_business.adobjects.adset'] = MagicMock()
sys.modules['facebook_business.adobjects.user'] = MagicMock()
sys.modules['facebook_business.adobjects.adspixel'] = MagicMock()
sys.modules['facebook_business.adobjects.campaign'] = MagicMock()
sys.modules['facebook_business.adobjects.business'] = MagicMock()

# Import MetaUploader (now mocking requests won't fail imports)
# We might need to mock os.getenv if it's used at module level, but it seems fine in class
from meta_api import MetaUploader

class TestMetaUploaderPayload(unittest.TestCase):
    def setUp(self):
        # Setup dummy uploader
        self.uploader = MetaUploader('act_123', 'token', 'app_id', 'app_secret')
        # Mock wait methods to prevent API calls during payload tests
        self.uploader.wait_for_video_ready = MagicMock(return_value=True)
        self.uploader.wait_for_image_ready = MagicMock(return_value=True)

    @patch('meta_api.requests.post')
    def test_dual_media(self, mock_post):
        """Testa cenário ideal: Feed + Stories distintos."""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.json.return_value = {'id': 'dummy_creative_id'}
        mock_post.return_value = mock_response

        feed_media = {'type': 'image', 'hash': 'HASH_FEED', 'id': None}
        stories_media = {'type': 'video', 'id': 'VID_STORY', 'hash': None, 'thumb_hash': 'STORY_THUMB'}
        
        # We need to capture the payload sent to requests.post
        creative_id = self.uploader.create_creative_with_placements(
            'page123', feed_media, stories_media, 'http://link.com', ['BodyText'], ['Headline'], 'LEARN_MORE'
        )
        
        # Verify post was called
        mock_post.assert_called_once()
        
        # The payload is in kwargs['data']['asset_feed_spec'] (as a json string)
        kwargs = mock_post.call_args.kwargs
        post_data = kwargs.get('data', {})
        self.assertIn('asset_feed_spec', post_data)
        
        spec = json.loads(post_data['asset_feed_spec'])
        images = spec['images']
        videos = spec['videos']
        
        # Check assets
        self.assertEqual(len(images), 1) # FEED hash only, asset_feed_spec doesn't accept video thumbs
        
        # Find feed image label
        feed_img = next(i for i in images if i['hash'] == 'HASH_FEED')
        self.assertEqual(feed_img['adlabels'][0]['name'], 'feed_creative')
        
        self.assertEqual(len(videos), 1)
        self.assertEqual(videos[0]['video_id'], 'VID_STORY')
        self.assertEqual(videos[0]['adlabels'][0]['name'], 'story_creative')
        
        # Check Rules
        rules = spec['asset_customization_rules']
        self.assertEqual(len(rules), 2)

    @patch('meta_api.requests.post')
    def test_fallback_feed_only(self, mock_post):
        """Testa se apenas Feed cria regra para Stories também."""
        mock_response = MagicMock()
        mock_response.json.return_value = {'id': 'dummy_creative_id'}
        mock_post.return_value = mock_response

        feed_media = {'type': 'image', 'hash': 'HASH_FEED_ONLY', 'id': None}
        stories_media = None # Simulating missing stories

        # Should NOT raise error, should fallback
        creative_id = self.uploader.create_creative_with_placements(
            'page123', feed_media, stories_media, 'http://link.com', ['BodyText'], ['Headline'], 'LEARN_MORE'
        )
        
        kwargs = mock_post.call_args.kwargs
        post_data = kwargs.get('data', {})
        spec = json.loads(post_data['asset_feed_spec'])
        images = spec['images']
        
        # Should have 2 entries in images (one for each label) OR 1 entry with multiple labels
        # Current implementation: adds twice to the list, one per label call to add_media
        self.assertEqual(len(images), 2) 
        self.assertEqual(images[0]['hash'], 'HASH_FEED_ONLY')
        self.assertEqual(images[1]['hash'], 'HASH_FEED_ONLY')
        
        # Check labels
        labels = [img['adlabels'][0]['name'] for img in images]
        self.assertIn('feed_creative', labels)
        self.assertIn('story_creative', labels)

        # Check Rules existence
        rules = spec['asset_customization_rules']
        self.assertEqual(len(rules), 2)

    @patch('meta_api.requests.post')
    def test_fallback_stories_only(self, mock_post):
        """Testa se apenas Stories cria regra para Feed também."""
        mock_response = MagicMock()
        mock_response.json.return_value = {'id': 'dummy_creative_id'}
        mock_post.return_value = mock_response

        feed_media = None
        stories_media = {'type': 'video', 'id': 'VID_STORY_ONLY', 'hash': None, 'thumb_hash': 'THUMB_STORY'}

        creative_id = self.uploader.create_creative_with_placements(
            'page123', feed_media, stories_media, 'http://link.com', ['BodyText'], ['Headline'], 'LEARN_MORE'
        )

        kwargs = mock_post.call_args.kwargs
        post_data = kwargs.get('data', {})
        spec = json.loads(post_data['asset_feed_spec'])
        videos = spec['videos']
        
        self.assertEqual(len(videos), 2)
        self.assertEqual(videos[0]['video_id'], 'VID_STORY_ONLY')
        
        labels = [v['adlabels'][0]['name'] for v in videos]
        self.assertIn('feed_creative', labels)
        self.assertIn('story_creative', labels)

if __name__ == '__main__':
    print("\n🚀 Iniciando verificação de payloads...\n")
    unittest.main(exit=False)
