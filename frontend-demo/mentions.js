(function exposeMentionParser(global) {
  function extractMentions(text) {
    const seen = new Set();
    const mentions = [];
    const pattern = /(^|[^\p{L}\p{N}_.:-])@([A-Za-z0-9_.:-]+)/gu;
    let match;
    while ((match = pattern.exec(text)) !== null) {
      const agentId = match[2];
      if (seen.has(agentId)) continue;
      seen.add(agentId);
      mentions.push({ agentId, label: agentId });
    }
    return mentions;
  }

  global.extractMentions = extractMentions;
})(globalThis);
