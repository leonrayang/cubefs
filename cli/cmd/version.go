package cmd

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
	"time"
)

const (
	cmdVersionUse   = "version [COMMAND]"
	cmdVersionShort = "Manage cluster volumes versions"
	cmdVersionCreateShort = "create volume version"
	cmdVersionDelShort = "del volume version"
	cmdVersionListShort = "list volume version"
)
func newVersionCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:     cmdVersionUse,
		Short:   cmdVersionShort,
		Args:    cobra.MinimumNArgs(0),
		Aliases: []string{"version"},
	}
	cmd.AddCommand(
		newVersionCreateCmd(client),
		newVersionDelCmd(client),
		newVersionListCmd(client),
	)
	return cmd
}

func newVersionCreateCmd(client *master.MasterClient) *cobra.Command {
	var optKeyword string
	var cmd = &cobra.Command{
		Use:     CliFlagVersionCreate,
		Short:   cmdVersionCreateShort,
		Aliases: []string{"create"},
		Run: func(cmd *cobra.Command, args []string) {
			var ver *proto.VolVersionInfo
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			if ver, err = client.AdminAPI().CreateVersion(optKeyword); err != nil {
				return
			}

			stdout("%v\n", fmt.Sprintf("version info[verseq %v, time %v, status %v",
				ver.Status, ver.Ctime.Format(time.UnixDate), ver.Status))
		},
	}
	cmd.Flags().StringVar(&optKeyword, "keyword", "", "Specify keyword of volume name to filter")
	return cmd
}

func newVersionListCmd(client *master.MasterClient) *cobra.Command {
	var optKeyword string
	var cmd = &cobra.Command{
		Use:     CliFlagVersionList,
		Short:   cmdVersionListShort,
		Aliases: []string{"create"},
		Run: func(cmd *cobra.Command, args []string) {
			var verList *proto.VolVersionInfoList
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			if verList, err = client.AdminAPI().GetVerList(optKeyword); err != nil {
				return
			}
			stdout("%v\n", volumeInfoTableHeader)
			for _, ver := range verList.VerList {
				stdout("%v\n", fmt.Sprintf("verseq %v, time %v, status %v",
					ver.Status, ver.Ctime.Format(time.UnixDate), ver.Status))
			}
		},
	}
	cmd.Flags().StringVar(&optKeyword, "keyword", "", "Specify keyword of volume name to filter")
	return cmd
}

func newVersionDelCmd(client *master.MasterClient) *cobra.Command {
	var optKeyword string
	var cmd = &cobra.Command{
		Use:     CliFlagVersionDel,
		Short:   cmdVersionDelShort,
		Aliases: []string{"create"},
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			if err = client.AdminAPI().DeleteVersion(optKeyword); err != nil {
				return
			}
		},
	}
	cmd.Flags().StringVar(&optKeyword, "keyword", "", "Specify keyword of volume name to filter")
	return cmd
}