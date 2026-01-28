import React, { useEffect, useState, useCallback } from 'react';
import closeIcon from '@iconify-icons/eva/close-outline';
import linkIcon from '@iconify-icons/mdi/link-variant';
import deleteIcon from '@iconify-icons/mdi/delete';

import {
  Box,
  alpha,
  Dialog,
  Button,
  useTheme,
  Typography,
  IconButton,
  DialogTitle,
  DialogContent,
  DialogActions,
  Chip,
  Autocomplete,
  TextField,
  CircularProgress,
  Alert,
  Stack,
} from '@mui/material';

import { Iconify } from 'src/components/iconify';
import { KnowledgeBaseAPI } from '../../services/api';

interface KBLink {
  id: string;
  name: string;
  createdAtTimestamp: number;
  createdBy: string;
  edgeId: string;
}

interface KnowledgeBase {
  id: string;
  name: string;
  createdAtTimestamp?: number;
  updatedAtTimestamp?: number;
  userRole?: string;
}

interface LinkToKBDialogProps {
  open: boolean;
  onClose: () => void;
  recordId: string;
  recordName: string;
}

export const LinkToKBDialog: React.FC<LinkToKBDialogProps> = ({
  open,
  onClose,
  recordId,
  recordName,
}) => {
  const theme = useTheme();
  const [kbLinks, setKbLinks] = useState<KBLink[]>([]);
  const [accessibleKBs, setAccessibleKBs] = useState<KnowledgeBase[]>([]);
  const [selectedKBs, setSelectedKBs] = useState<KnowledgeBase[]>([]);
  const [loading, setLoading] = useState(false);
  const [loadingLinks, setLoadingLinks] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [removingLinkId, setRemovingLinkId] = useState<string | null>(null);

  const loadData = useCallback(async () => {
    setLoadingLinks(true);
    setError(null);
    try {
      // Load existing KB links
      const links = await KnowledgeBaseAPI.getRecordKBLinks(recordId);
      setKbLinks(links || []);

      // Load accessible KBs
      const kbsResponse = await KnowledgeBaseAPI.getAccessibleKBs({
        page: 1,
        limit: 100,
      });

      // Handle different response formats
      let kbs: KnowledgeBase[] = [];
      if (kbsResponse.knowledgeBases) {
        kbs = kbsResponse.knowledgeBases.map((kb: any) => ({
          id: kb.id,
          name: kb.name,
          createdAtTimestamp: kb.createdAtTimestamp,
          updatedAtTimestamp: kb.updatedAtTimestamp,
          userRole: kb.userRole,
        }));
      } else if (Array.isArray(kbsResponse)) {
        kbs = kbsResponse.map((kb: any) => ({
          id: kb.id,
          name: kb.name,
          createdAtTimestamp: kb.createdAtTimestamp,
          updatedAtTimestamp: kb.updatedAtTimestamp,
          userRole: kb.userRole,
        }));
      }

      // Filter out KBs that are already linked
      const linkedKbIds = new Set(links.map((link: KBLink) => link.id));
      const availableKBs = kbs.filter((kb) => !linkedKbIds.has(kb.id));
      setAccessibleKBs(availableKBs);
    } catch (err: any) {
      setError(err?.message || 'Failed to load data');
      console.error('Error loading KB links:', err);
    } finally {
      setLoadingLinks(false);
    }
  }, [recordId]);

  // Load KB links and accessible KBs when dialog opens
  useEffect(() => {
    if (open) {
      loadData();
    } else {
      // Reset state when dialog closes
      setKbLinks([]);
      setAccessibleKBs([]);
      setSelectedKBs([]);
      setError(null);
    }
  }, [open, recordId, loadData]);

  const handleAddLinks = async () => {
    if (!selectedKBs || selectedKBs.length === 0) return;

    setLoading(true);
    setError(null);
    try {
      // Link all selected KBs
      const linkPromises = selectedKBs.map((kb) =>
        KnowledgeBaseAPI.linkRecordToKB(recordId, kb.id)
      );
      await Promise.all(linkPromises);
      
      // Reload data to refresh the list
      await loadData();
      setSelectedKBs([]);
    } catch (err: any) {
      setError(err?.message || 'Failed to link records to KBs');
      console.error('Error linking records to KBs:', err);
    } finally {
      setLoading(false);
    }
  };

  const handleRemoveLink = async (kbId: string) => {
    setRemovingLinkId(kbId);
    setError(null);
    try {
      await KnowledgeBaseAPI.unlinkRecordFromKB(recordId, kbId);
      // Reload data to refresh the list
      await loadData();
    } catch (err: any) {
      setError(err?.message || 'Failed to remove link');
      console.error('Error removing KB link:', err);
    } finally {
      setRemovingLinkId(null);
    }
  };

  return (
    <Dialog
      open={open}
      onClose={onClose}
      maxWidth="sm"
      fullWidth
      BackdropProps={{
        sx: {
          backdropFilter: 'blur(1px)',
          backgroundColor: alpha(theme.palette.common.black, 0.3),
        },
      }}
      PaperProps={{
        sx: {
          borderRadius: 1,
          boxShadow: '0 10px 35px rgba(0, 0, 0, 0.1)',
          overflow: 'hidden',
        },
      }}
    >
      <DialogTitle
        sx={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          p: 2.5,
          pl: 3,
          color: theme.palette.text.primary,
          borderBottom: '1px solid',
          borderColor: theme.palette.divider,
          fontWeight: 500,
          fontSize: '1rem',
          m: 0,
        }}
      >
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
          <Box
            sx={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              width: 32,
              height: 32,
              borderRadius: '6px',
              bgcolor: alpha(theme.palette.primary.main, 0.1),
              color: theme.palette.primary.main,
            }}
          >
            <Iconify icon={linkIcon} width={18} height={18} />
          </Box>
          Add to Knowledge Base
        </Box>

        <IconButton
          onClick={onClose}
          size="small"
          sx={{ color: theme.palette.text.secondary }}
          aria-label="close"
          disabled={loading || loadingLinks}
        >
          <Iconify icon={closeIcon} width={20} height={20} />
        </IconButton>
      </DialogTitle>

      <DialogContent
        sx={{
          p: 0,
          '&.MuiDialogContent-root': {
            pt: 3,
            px: 3,
            pb: 0,
          },
        }}
      >
        <Box sx={{ mb: 3 }}>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Manage Knowledge Base links for <strong>{recordName}</strong>
          </Typography>

          {error && (
            <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>
              {error}
            </Alert>
          )}

          {/* Existing KB Links */}
          <Box sx={{ mb: 3 }}>
            <Typography variant="subtitle2" sx={{ mb: 1.5, fontWeight: 600 }}>
              Linked Knowledge Bases
            </Typography>
            {loadingLinks ? (
              <Box sx={{ display: 'flex', justifyContent: 'center', py: 2 }}>
                <CircularProgress size={24} />
              </Box>
            ) : kbLinks.length === 0 ? (
              <Typography variant="body2" color="text.secondary" sx={{ fontStyle: 'italic' }}>
                No Knowledge Bases linked yet
              </Typography>
            ) : (
              <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
                {kbLinks.map((link) => (
                  <Chip
                    key={link.id}
                    label={link.name}
                    onDelete={() => handleRemoveLink(link.id)}
                    deleteIcon={
                      removingLinkId === link.id ? (
                        <CircularProgress size={16} />
                      ) : (
                        <Iconify icon={deleteIcon} width={16} height={16} />
                      )
                    }
                    disabled={removingLinkId === link.id || loading}
                    sx={{
                      '& .MuiChip-deleteIcon': {
                        color: theme.palette.error.main,
                        '&:hover': {
                          color: theme.palette.error.dark,
                        },
                      },
                    }}
                  />
                ))}
              </Stack>
            )}
          </Box>

          {/* Add New Links */}
          <Box>
            <Typography variant="subtitle2" sx={{ mb: 1.5, fontWeight: 600 }}>
              Add to Knowledge Bases
            </Typography>
            <Autocomplete
              multiple
              options={accessibleKBs}
              getOptionLabel={(option) => option.name}
              value={selectedKBs}
              onChange={(_, newValue) => setSelectedKBs(newValue)}
              disabled={loading || loadingLinks || accessibleKBs.length === 0}
              renderInput={(params) => (
                <TextField
                  {...params}
                  placeholder={
                    accessibleKBs.length === 0
                      ? 'No available Knowledge Bases'
                      : 'Select Knowledge Bases'
                  }
                  size="small"
                />
              )}
              renderTags={(value, getTagProps) =>
                value.map((option, index) => (
                  <Chip
                    {...getTagProps({ index })}
                    key={option.id}
                    label={option.name}
                    size="small"
                  />
                ))
              }
              sx={{ mb: 2 }}
            />
            <Button
              variant="contained"
              onClick={handleAddLinks}
              disabled={selectedKBs.length === 0 || loading || loadingLinks}
              startIcon={loading ? <CircularProgress size={16} /> : <Iconify icon={linkIcon} width={16} />}
              sx={{
                bgcolor: theme.palette.primary.main,
                boxShadow: 'none',
                fontWeight: 500,
                '&:hover': {
                  bgcolor: theme.palette.primary.dark,
                  boxShadow: 'none',
                },
                '&.Mui-disabled': {
                  bgcolor: alpha(theme.palette.primary.main, 0.3),
                  color: alpha(theme.palette.primary.contrastText, 0.5),
                },
              }}
            >
              {loading ? 'Linking...' : `Add ${selectedKBs.length > 0 ? `${selectedKBs.length} ` : ''}Link${selectedKBs.length !== 1 ? 's' : ''}`}
            </Button>
          </Box>
        </Box>
      </DialogContent>

      <DialogActions
        sx={{
          p: 2.5,
          borderTop: '1px solid',
          borderColor: theme.palette.divider,
          bgcolor: alpha(theme.palette.background.default, 0.5),
        }}
      >
        <Button
          variant="text"
          onClick={onClose}
          disabled={loading || loadingLinks}
          sx={{
            color: theme.palette.text.secondary,
            fontWeight: 500,
            '&:hover': {
              backgroundColor: alpha(theme.palette.divider, 0.8),
            },
          }}
        >
          Close
        </Button>
      </DialogActions>
    </Dialog>
  );
};
