pub trait Transport {
    fn get_settings(
        &mut self,
        block_id: BlockId,
        keys: Vec<String>,
    ) -> Result<HashMap<String, String>, Error> {
        let mut request = ConsensusSettingsGetRequest::new();
        request.set_block_id(block_id);
        request.set_keys(protobuf::RepeatedField::from_vec(keys));

        let mut response: ConsensusSettingsGetResponse = self.rpc(
            &request,
            Message_MessageType::CONSENSUS_SETTINGS_GET_REQUEST,
            Message_MessageType::CONSENSUS_SETTINGS_GET_RESPONSE,
        )?;

        if response.get_status() == ConsensusSettingsGetResponse_Status::UNKNOWN_BLOCK {
            Err(Error::UnknownBlock("Block not found".into()))
        } else {
            check_ok!(response, ConsensusSettingsGetResponse_Status::OK)
        }?;

        Ok(response
            .take_entries()
            .into_iter()
            .map(|mut entry| (entry.take_key(), entry.take_value()))
            .collect())
    }
}